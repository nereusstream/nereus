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

import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.domain.protocol.PulsarBindingGeneration;
import com.nereusstream.domain.protocol.PulsarPersistenceName;
import com.nereusstream.domain.protocol.PulsarTopicIncarnationIdentity;
import com.nereusstream.domain.protocol.PulsarTopicName;
import com.nereusstream.metadata.oxia.v2.key.OxiaV2AuthorityKeys;
import com.nereusstream.metadata.oxia.v2.retention.Oxia09ExactMetadataTransactionStoreV1;
import com.nereusstream.pulsar.offload.PulsarOffloadKeysV1;
import com.nereusstream.storage.object.retention.M5PulsarAggregateAuthorityCodecV1;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;
import org.apache.bookkeeper.mledger.proto.BookKeeperDeleteState;
import org.apache.bookkeeper.mledger.proto.BookKeeperRetentionClass;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class M6PulsarBookKeeperReplacementProbe {
    private static final String NAMESPACE = "public/m6-native";
    private static final String TOPIC = "persistent://" + NAMESPACE + "/handoff";
    private static final String SUB = "m6-replacement-backlog";
    private static final int COUNT = 6;

    public static void main(String[] args) throws Exception {
        var state = Path.of(args[4]);
        var cell = new Properties();
        try (var file = Files.newBufferedReader(Path.of(args[args.length - 1]))) { cell.load(file); }
        try (var meta = MetadataStoreFactory.create(args[1], MetadataStoreConfig.builder().build());
                var client = PulsarClient.builder().serviceUrl("pulsar://" + args[3])
                        .operationTimeout(20, TimeUnit.SECONDS).build();
                var admin = PulsarAdmin.builder().serviceHttpUrl("http://" + args[2]).build();
                var bookKeeper = new BookKeeper(new ClientConfiguration().setMetadataServiceUri(args[5]))) {
            if ("initial".equals(args[0])) {
                admin.namespaces().createNamespace(NAMESPACE, 1);
                admin.namespaces().setRetention(NAMESPACE, new RetentionPolicies(-1, -1));
                admin.topics().createNonPartitionedTopic(TOPIC);
                var positions = new ArrayList<MessageId>();
                try (var subscription = client.newConsumer().topic(TOPIC).subscriptionName(SUB)
                                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe();
                        var producer = client.newProducer().topic(TOPIC).enableBatching(false).create()) {
                    for (int i = 0; i < COUNT; i++) {
                        positions.add(producer.send(body(i)));
                    }
                }
                long ledger = ((MessageIdAdv) positions.getFirst()).getLedgerId();
                if (((MessageIdAdv) positions.get(2)).getLedgerId() == ledger) {
                    throw new AssertionError("No real native rollover before offload");
                }
                var fault = Path.of(args[6]);
                try (var oxia = OxiaClientBuilder.create(cell.getProperty("oxiaAddress"))
                        .namespace(cell.getProperty("oxiaNamespace")).asyncClient().get(10, TimeUnit.SECONDS)) {
                    var prior = mutationTickets(oxia, cell);
                    Files.createFile(fault.resolve("armed"));
                    admin.topics().triggerOffload(TOPIC, positions.get(2));
                    waitForApplied(fault);
                    var pending = new java.util.HashSet<>(mutationTickets(oxia, cell));
                    pending.removeAll(prior);
                    if (pending.isEmpty() || !context(meta, ledger).isComplete()
                            || admin.topics().offloadStatus(TOPIC).getStatus()
                                    != LongRunningProcessStatus.Status.RUNNING) {
                        throw new AssertionError("Original publication did not hold its ticket and client completion");
                    }
                    Files.createFile(fault.resolve("release"));
                    awaitOffload(admin);
                    var remaining = mutationTickets(oxia, cell);
                    if (pending.stream().anyMatch(remaining::contains) || Files.exists(fault.resolve("repeated"))) {
                        throw new AssertionError("Exact original publication did not settle its tickets without retry");
                    }
                    Files.delete(fault.resolve("armed"));
                    System.out.println("PULSAR_M6_NATIVE_PUBLICATION_UNKNOWN_SETTLED " + ledger);
                }
                var before = context(meta, ledger);
                if (!before.isComplete() || before.isBookkeeperDeleted()
                        || before.getBookkeeperRetentionClass() != BookKeeperRetentionClass.DELETE_AFTER_VERIFIED
                        || before.getBookkeeperDeleteState() != BookKeeperDeleteState.BK_DELETE_NONE) {
                    throw new AssertionError("Original Broker native offload marker is not eligible");
                }
                if (!bookKeeper.getLedgerManager().readLedgerMetadata(ledger).get(10, TimeUnit.SECONDS)
                        .getValue().isClosed()) throw new AssertionError("Original BookKeeper source is not sealed");
                if (admin.topics().getStats(TOPIC, true).getSubscriptions().get(SUB).getMsgBacklog() != COUNT) {
                    throw new AssertionError("Original Broker durable backlog differs before replacement");
                }
                var expected = new Properties();
                expected.setProperty("ledger", Long.toString(ledger));
                expected.setProperty("attempt", before.getUidMsb() + ":" + before.getUidLsb());
                requireOriginalObjectVersions(cell, ledger, new UUID(before.getUidMsb(), before.getUidLsb()), expected, false);
                for (int i = 0; i < COUNT; i++) {
                    expected.setProperty("position." + i,
                            Base64.getEncoder().encodeToString(positions.get(i).toByteArray()));
                }
                try (var file = Files.newBufferedWriter(state)) { expected.store(file, null); }
                System.out.println("PULSAR_M6_DELETE_ENROLLED_OUTPUT " + ledger);
            } else {
                var expected = new Properties();
                try (var file = Files.newBufferedReader(state)) { expected.load(file); }
                long ledger = Long.parseLong(expected.getProperty("ledger"));
                var after = context(meta, ledger);
                if (!after.isComplete() || !after.isBookkeeperDeleted()
                        || after.getBookkeeperDeleteState() != BookKeeperDeleteState.BK_DELETE_DONE
                        || !expected.getProperty("attempt").equals(after.getUidMsb() + ":" + after.getUidLsb())) {
                    throw new AssertionError("Native DONE did not retain original offload identity");
                }
                requireOriginalObjectVersions(cell, ledger, new UUID(after.getUidMsb(), after.getUidLsb()), expected, true);
                try {
                    bookKeeper.getLedgerManager().readLedgerMetadata(ledger).get(10, TimeUnit.SECONDS);
                    throw new AssertionError("Native DONE still has physical BookKeeper ledger metadata");
                } catch (java.util.concurrent.ExecutionException failure) {
                    if (!(failure.getCause() instanceof BKException.BKNoSuchLedgerExistsOnMetadataServerException)) {
                        throw failure;
                    }
                }
                if (admin.topics().getStats(TOPIC, true).getSubscriptions().get(SUB).getMsgBacklog() != COUNT) {
                    throw new AssertionError("Cold Broker durable backlog differs");
                }
                try (var subscription = client.newConsumer().topic(TOPIC).subscriptionName(SUB).subscribe()) {
                    for (int i = 0; i < COUNT; i++) {
                        var entry = subscription.receive(15, TimeUnit.SECONDS);
                        var expectedId = MessageId.fromByteArray(
                                Base64.getDecoder().decode(expected.getProperty("position." + i)));
                        if (entry == null || !entry.getMessageId().equals(expectedId)
                                || !Arrays.equals(entry.getData(), body(i))) {
                            throw new AssertionError("Cold Broker changed entry or MessageId at ordinal " + i);
                        }
                    }
                }
                System.out.println("PULSAR_M6_DELETE_COLD_BACKLOG_PASS " + ledger);
            }
        }
    }

    private static byte[] body(int ordinal) {
        return ("native broker durable entry " + ordinal).getBytes(StandardCharsets.UTF_8);
    }

    private static void waitForApplied(Path fault) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!Files.isRegularFile(fault.resolve("applied"))) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Native publication did not reach its real backend before reply loss");
            }
            Thread.sleep(20);
        }
    }

    private static void awaitOffload(PulsarAdmin admin) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true) {
            var status = admin.topics().offloadStatus(TOPIC);
            if (status.getStatus() == LongRunningProcessStatus.Status.SUCCESS) return;
            if (status.getStatus() == LongRunningProcessStatus.Status.ERROR || System.nanoTime() > deadline) {
                throw new AssertionError("Original Broker offload failed: " + status.getLastError());
            }
            Thread.sleep(100);
        }
    }

    private static Set<Sha256Digest> mutationTickets(AsyncOxiaClient oxia, Properties cell) throws Exception {
        var name = TopicName.get(TOPIC);
        var incarnation = new PulsarTopicIncarnationIdentity(
                PulsarPersistenceName.fromString(name.getPersistenceNamingEncoding()),
                PulsarTopicName.fromString(TOPIC),
                new PulsarBindingGeneration(Long.parseLong(cell.getProperty("binding." + TOPIC))));
        var key = new OxiaV2AuthorityKeys(cell.getProperty("authorityRoot")).aggregateKey(incarnation);
        var exact = new Oxia09ExactMetadataTransactionStoreV1(oxia).read(key).toCompletableFuture()
                .get(10, TimeUnit.SECONDS).orElseThrow();
        return M5PulsarAggregateAuthorityCodecV1.decodeAuthority(exact.canonicalStoredBytes())
                .referenceMutationTickets().stream().map(ticket -> ticket.operationIdSha256())
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private static void requireOriginalObjectVersions(
            Properties cell, long ledger, UUID attempt, Properties observed, boolean absent) {
        var keys = PulsarOffloadKeysV1.derive(cell.getProperty("s3KeyPrefix"), ledger, attempt);
        try (var s3 = S3Client.builder().endpointOverride(URI.create(cell.getProperty("s3Endpoint")))
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create()).forcePathStyle(true)
                .region(Region.of(cell.getProperty("s3Region"))).build()) {
            var exact = s3.listObjectVersions(request -> request.bucket(cell.getProperty("s3Bucket"))
                    .prefix(keys.attemptPrefix() + "/"));
            if (exact.isTruncated() || !exact.deleteMarkers().isEmpty()) {
                throw new AssertionError("Original offload is not one bounded exact-version inventory");
            }
            if (absent) {
                if (observed.getProperty("original.dataVersion") == null
                        || observed.getProperty("original.rootVersion") == null || !exact.versions().isEmpty()) {
                    throw new AssertionError("Original NPD1/NPO1 immutable versions remain after settlement");
                }
                return;
            }
            var root = exact.versions().stream().filter(version -> version.key().equals(keys.rootKey()))
                    .map(version -> version.versionId()).findFirst().orElseThrow();
            var data = exact.versions().stream().filter(version -> version.key().equals(keys.dataKey()))
                    .map(version -> version.versionId()).findFirst().orElseThrow();
            if (exact.versions().size() != 2 || "null".equals(root) || "null".equals(data)) {
                throw new AssertionError("Original native offload did not produce exactly one immutable version per object");
            }
            observed.setProperty("original.rootVersion", root);
            observed.setProperty("original.dataVersion", data);
            observed.setProperty("original.rootKey", keys.rootKey());
            observed.setProperty("original.dataKey", keys.dataKey());
        }
    }

    private static org.apache.bookkeeper.mledger.proto.OffloadContext context(MetadataStore store, long ledger)
            throws Exception {
        var path = "/managed-ledgers/" + TopicName.get(TOPIC).getPersistenceNamingEncoding();
        var raw = store.get(path).get(10, TimeUnit.SECONDS).orElseThrow().getValue();
        return new MetaStoreImpl(null, null).parseManagedLedgerInfo(raw).getLedgerInfosList().stream()
                .filter(row -> row.getLedgerId() == ledger).findFirst().orElseThrow().getOffloadContext();
    }
}
