/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.nereusstream.api.StreamId;
import com.nereusstream.bookkeeper.BookKeeperDigestType;
import com.nereusstream.bookkeeper.BookKeeperLedgerIdNamespaceProvisioningCoordinator;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationCoordinator;
import com.nereusstream.bookkeeper.BookKeeperProtocolActivationUpdate;
import com.nereusstream.bookkeeper.BookKeeperSecretRef;
import com.nereusstream.bookkeeper.BookKeeperWalConfiguration;
import com.nereusstream.bookkeeper.OxiaBookKeeperLedgerIdNamespaceReservationStore;
import com.nereusstream.bookkeeper.OxiaBookKeeperProtocolActivationStore;
import com.nereusstream.metadata.oxia.BookKeeperKeyspace;
import com.nereusstream.metadata.oxia.BookKeeperMetadataStoreConfig;
import com.nereusstream.metadata.oxia.BookKeeperScanToken;
import com.nereusstream.metadata.oxia.BookKeeperVersionedValue;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.KafkaStorageActivationMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaBookKeeperMetadataStore;
import com.nereusstream.metadata.oxia.OxiaJavaKafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.records.AppendReservationLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperAppendReservationRecord;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerLifecycle;
import com.nereusstream.metadata.oxia.records.BookKeeperLedgerRootRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageActivationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaStorageProtocolActivationRecord;
import com.nereusstream.metadata.oxia.records.KafkaStorageReadinessRecord;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.oxia.testcontainers.OxiaContainer;
import java.io.IOException;
import java.io.Writer;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

/**
 * Starts the product launcher in a separate OS process over real Oxia and an S3-compatible provider.
 *
 * <p>This is intentionally not a Kafka test-kit fixture: formatting, feature activation, broker registration,
 * controller policy enforcement, provider creation, Produce, Fetch, ListOffsets, group/transaction coordination and
 * shutdown all cross the same launcher and scripts that an operator uses. Fresh JVMs must reacquire readiness at higher
 * broker epochs, recover remote user/internal-topic state, and resolve an open transaction left by a forced process exit.
 */
@Testcontainers
class NereusKafkaNativeProcessIntegrationTest {
    private static final DockerImageName OXIA_IMAGE =
            DockerImageName.parse("oxia/oxia:0.16.3");
    private static final DockerImageName LOCALSTACK_IMAGE =
            DockerImageName.parse("localstack/localstack:4.14.0");
    private static final DockerImageName TOXIPROXY_IMAGE =
            DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0");
    private static final Duration PROCESS_TIMEOUT = Duration.ofSeconds(90);
    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(60);

    @Container
    private static final OxiaContainer OXIA =
            new OxiaContainer(OXIA_IMAGE).withShards(4);

    @Container
    private static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(LOCALSTACK_IMAGE)
                    .withServices(LocalStackContainer.Service.S3);

    @TempDir
    Path root;

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void productProcessRecoversUserGroupAndTransactionStateAcrossGracefulAndForcedRestarts()
            throws Exception {
        clearFailureEvidence();
        Path kafkaCheckout = requiredKafkaCheckout();
        Path kafkaHome = extractReleaseDistribution(
                kafkaCheckout,
                root.resolve("kafka-distribution"));
        Path formatScript = executable(kafkaHome.resolve("bin/kafka-storage.sh"));
        Path startScript = executable(kafkaHome.resolve("bin/nereus-kafka-server-start.sh"));
        Path config = root.resolve("server.properties");
        Path formatLog = root.resolve("format.log");
        Path firstServerLog = root.resolve("server-first.log");
        Path restartServerLog = root.resolve("server-restart.log");
        Path interruptedServerLog = root.resolve("server-interrupted-transaction.log");
        Path recoveryServerLog = root.resolve("server-transaction-recovery.log");
        String bucket = "nereus-kafka-" + UUID.randomUUID();
        String topic = "process-gate-" + UUID.randomUUID();
        String groupId = "process-group-" + UUID.randomUUID();
        String transactionalId = "process-transaction-" + UUID.randomUUID();
        String interruptedTransactionalId =
                "process-interrupted-transaction-" + UUID.randomUUID();
        int brokerPort = freePort();
        int controllerPort = differentFreePort(brokerPort);
        String bootstrapServers = "127.0.0.1:" + brokerPort;

        createBucket(bucket);
        writeConfiguration(
                config,
                brokerPort,
                controllerPort,
                bucket,
                root.resolve("kafka-log"),
                root.resolve("metadata-log"),
                root.resolve("nereus-cache"));

        Process format = start(
                List.of(
                        formatScript.toString(),
                        "format",
                        "--cluster-id",
                        org.apache.kafka.common.Uuid.randomUuid().toString(),
                        "--config",
                        config.toString(),
                        "--feature",
                        "nereus.storage.version=1"),
                kafkaHome,
                formatLog);
        try {
            int formatExit = await(format, PROCESS_TIMEOUT, "Kafka storage format", formatLog);
            assertThat(formatExit)
                    .withFailMessage(() -> "storage format failed:\n" + readLog(formatLog))
                    .isZero();
        } catch (Exception | AssertionError failure) {
            try {
                preserveFailureEvidence(
                        config,
                        formatLog,
                        firstServerLog,
                        restartServerLog,
                        interruptedServerLog,
                        recoveryServerLog);
            } catch (AssertionError evidenceFailure) {
                failure.addSuppressed(evidenceFailure);
            }
            throw failure;
        }

        TopicPartition partition = new TopicPartition(topic, 0);
        byte[] firstKey = "key-0".getBytes(StandardCharsets.UTF_8);
        byte[] firstValue = "nereus-native-process-first".getBytes(StandardCharsets.UTF_8);
        byte[] firstTransactionalKey = "key-tx-0".getBytes(StandardCharsets.UTF_8);
        byte[] firstTransactionalValue =
                "nereus-native-process-transaction-first".getBytes(StandardCharsets.UTF_8);
        runBroker(
                startScript,
                kafkaHome,
                config,
                formatLog,
                firstServerLog,
                bootstrapServers,
                () -> {
                    try (Admin admin = Admin.create(adminProperties(bootstrapServers))) {
                        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                                .all()
                                .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

                        RecordMetadata produced =
                                produce(bootstrapServers, topic, firstKey, firstValue);
                        assertThat(produced.partition()).isZero();
                        assertThat(produced.offset()).isZero();

                        RecordMetadata transactional = transactionalProduce(
                                bootstrapServers,
                                transactionalId,
                                topic,
                                firstTransactionalKey,
                                firstTransactionalValue);
                        assertThat(transactional.partition()).isZero();
                        assertThat(transactional.offset()).isEqualTo(1L);

                        ConsumerRecord<byte[], byte[]> fetched =
                                fetch(bootstrapServers, partition, 0L, firstServerLog);
                        assertThat(fetched.offset()).isZero();
                        assertThat(fetched.key()).isEqualTo(firstKey);
                        assertThat(fetched.value()).isEqualTo(firstValue);

                        ConsumerRecord<byte[], byte[]> fetchedTransaction =
                                fetch(bootstrapServers, partition, 1L, firstServerLog);
                        assertThat(fetchedTransaction.offset()).isEqualTo(1L);
                        assertThat(fetchedTransaction.key()).isEqualTo(firstTransactionalKey);
                        assertThat(fetchedTransaction.value())
                                .isEqualTo(firstTransactionalValue);

                        ConsumerRecord<byte[], byte[]> grouped = consumeGroupThroughOffset(
                                bootstrapServers,
                                groupId,
                                topic,
                                partition,
                                0L,
                                1L,
                                firstServerLog);
                        assertThat(grouped.offset()).isEqualTo(1L);
                        assertThat(committedGroupOffset(
                                bootstrapServers, groupId, partition))
                                .isEqualTo(2L);

                        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
                                admin.listOffsets(Map.of(partition, OffsetSpec.earliest()))
                                        .all()
                                        .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest =
                                admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                                        .all()
                                        .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                        assertThat(earliest.get(partition).offset()).isZero();
                        assertThat(latest.get(partition).offset())
                                .as("the committed transaction adds a control-marker offset")
                                .isEqualTo(3L);
                    }
                });

        byte[] secondTransactionalKey = "key-tx-1".getBytes(StandardCharsets.UTF_8);
        byte[] secondTransactionalValue =
                "nereus-native-process-transaction-restart".getBytes(StandardCharsets.UTF_8);
        runBroker(
                startScript,
                kafkaHome,
                config,
                formatLog,
                restartServerLog,
                bootstrapServers,
                () -> {
                    try (Admin admin = Admin.create(adminProperties(bootstrapServers))) {
                        ConsumerRecord<byte[], byte[]> recovered =
                                fetch(bootstrapServers, partition, 0L, restartServerLog);
                        assertThat(recovered.offset()).isZero();
                        assertThat(recovered.key()).isEqualTo(firstKey);
                        assertThat(recovered.value()).isEqualTo(firstValue);

                        ConsumerRecord<byte[], byte[]> recoveredTransaction =
                                fetch(bootstrapServers, partition, 1L, restartServerLog);
                        assertThat(recoveredTransaction.offset()).isEqualTo(1L);
                        assertThat(recoveredTransaction.key()).isEqualTo(firstTransactionalKey);
                        assertThat(recoveredTransaction.value())
                                .isEqualTo(firstTransactionalValue);

                        assertThat(committedGroupOffset(
                                bootstrapServers, groupId, partition))
                                .as("the recovered group coordinator must retain the committed offset")
                                .isEqualTo(2L);

                        RecordMetadata produced = transactionalProduce(
                                bootstrapServers,
                                transactionalId,
                                topic,
                                secondTransactionalKey,
                                secondTransactionalValue);
                        assertThat(produced.partition()).isZero();
                        assertThat(produced.offset())
                                .as("the recovered log must retain the first transaction marker")
                                .isEqualTo(3L);

                        ConsumerRecord<byte[], byte[]> fetched =
                                fetch(bootstrapServers, partition, 3L, restartServerLog);
                        assertThat(fetched.offset()).isEqualTo(3L);
                        assertThat(fetched.key()).isEqualTo(secondTransactionalKey);
                        assertThat(fetched.value()).isEqualTo(secondTransactionalValue);

                        ConsumerRecord<byte[], byte[]> grouped = consumeGroupThroughOffset(
                                bootstrapServers,
                                groupId,
                                topic,
                                partition,
                                3L,
                                3L,
                                restartServerLog);
                        assertThat(grouped.offset()).isEqualTo(3L);
                        assertThat(committedGroupOffset(
                                bootstrapServers, groupId, partition))
                                .isEqualTo(4L);

                        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
                                admin.listOffsets(Map.of(partition, OffsetSpec.earliest()))
                                        .all()
                                        .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest =
                                admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                                        .all()
                                        .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                        assertThat(earliest.get(partition).offset()).isZero();
                        assertThat(latest.get(partition).offset())
                                .as("both committed transactions retain their control markers")
                                .isEqualTo(5L);
                    }
                });

        byte[] interruptedKey = "key-tx-interrupted".getBytes(StandardCharsets.UTF_8);
        byte[] interruptedValue =
                "nereus-native-process-transaction-interrupted".getBytes(StandardCharsets.UTF_8);
        AtomicReference<OpenTransaction> interrupted = new AtomicReference<>();
        try {
            runBroker(
                    startScript,
                    kafkaHome,
                    config,
                    formatLog,
                    interruptedServerLog,
                    bootstrapServers,
                    StopMode.FORCE,
                    () -> {
                        OpenTransaction open = beginTransaction(
                                bootstrapServers,
                                interruptedTransactionalId,
                                topic,
                                interruptedKey,
                                interruptedValue);
                        interrupted.set(open);
                        assertThat(open.metadata().offset())
                                .as("the open transaction starts after two committed markers")
                                .isEqualTo(5L);

                        ConsumerRecord<byte[], byte[]> uncommitted =
                                fetch(bootstrapServers, partition, 5L, interruptedServerLog);
                        assertThat(uncommitted.offset()).isEqualTo(5L);
                        assertThat(uncommitted.key()).isEqualTo(interruptedKey);
                        assertThat(uncommitted.value()).isEqualTo(interruptedValue);
                    });
        } finally {
            OpenTransaction open = interrupted.get();
            if (open != null) {
                open.close();
            }
        }

        byte[] recoveredTransactionKey =
                "key-tx-after-interruption".getBytes(StandardCharsets.UTF_8);
        byte[] recoveredTransactionValue =
                "nereus-native-process-transaction-after-interruption"
                        .getBytes(StandardCharsets.UTF_8);
        runBroker(
                startScript,
                kafkaHome,
                config,
                formatLog,
                recoveryServerLog,
                bootstrapServers,
                () -> {
                    try (Admin admin = Admin.create(adminProperties(bootstrapServers))) {
                        assertThat(committedGroupOffset(
                                bootstrapServers, groupId, partition))
                                .as("the forced process exit cannot roll back the prior group commit")
                                .isEqualTo(4L);

                        RecordMetadata committed = transactionalProduce(
                                bootstrapServers,
                                interruptedTransactionalId,
                                topic,
                                recoveredTransactionKey,
                                recoveredTransactionValue);
                        assertThat(committed.offset())
                                .as("producer epoch recovery must abort the interrupted transaction first")
                                .isEqualTo(7L);

                        ConsumerRecord<byte[], byte[]> visible =
                                fetchReadCommitted(
                                        bootstrapServers,
                                        partition,
                                        5L,
                                        recoveryServerLog);
                        assertThat(visible.offset())
                                .as("read committed must skip interrupted data and its abort marker")
                                .isEqualTo(7L);
                        assertThat(visible.key()).isEqualTo(recoveredTransactionKey);
                        assertThat(visible.value()).isEqualTo(recoveredTransactionValue);

                        ConsumerRecord<byte[], byte[]> grouped = consumeGroupThroughOffset(
                                bootstrapServers,
                                groupId,
                                topic,
                                partition,
                                7L,
                                7L,
                                recoveryServerLog);
                        assertThat(grouped.offset()).isEqualTo(7L);
                        assertThat(committedGroupOffset(
                                bootstrapServers, groupId, partition))
                                .isEqualTo(8L);

                        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
                                admin.listOffsets(Map.of(partition, OffsetSpec.earliest()))
                                        .all()
                                        .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest =
                                admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                                        .all()
                                        .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                        assertThat(earliest.get(partition).offset()).isZero();
                        assertThat(latest.get(partition).offset())
                                .as("abort and commit markers are both retained in the logical log")
                                .isEqualTo(9L);
                    }
                });

        assertThat(objectCount(bucket))
                .as("user and internal-topic commits must persist S3-compatible objects")
                .isPositive();
    }

    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)
    void twoReleaseProcessesAtomicallyReassignLiveSharedStorageLeader()
            throws Exception {
        clearFailureEvidence();
        Path kafkaCheckout = requiredKafkaCheckout();
        Path kafkaHome = extractReleaseDistribution(
                kafkaCheckout,
                root.resolve("kafka-multi-broker-distribution"));
        Path formatScript = executable(kafkaHome.resolve("bin/kafka-storage.sh"));
        Path startScript = executable(kafkaHome.resolve("bin/nereus-kafka-server-start.sh"));
        Path brokerOneConfig = root.resolve("multi-broker-one.properties");
        Path brokerTwoConfig = root.resolve("multi-broker-two.properties");
        Path brokerOneFormatLog = root.resolve("multi-broker-one-format.log");
        Path brokerTwoFormatLog = root.resolve("multi-broker-two-format.log");
        Path brokerOneServerLog = root.resolve("multi-broker-one-server.log");
        Path brokerTwoServerLog = root.resolve("multi-broker-two-server.log");
        String bucket = "nereus-kafka-takeover-" + UUID.randomUUID();
        String topic = "process-takeover-" + UUID.randomUUID();
        String nereusCluster = "f9-process-takeover-" + UUID.randomUUID();
        String kafkaClusterId =
                org.apache.kafka.common.Uuid.randomUuid().toString();
        int brokerOnePort = freePort();
        int controllerPort = differentFreePort(brokerOnePort);
        int brokerTwoPort = differentFreePort(brokerOnePort, controllerPort);
        String brokerOneBootstrap = "127.0.0.1:" + brokerOnePort;
        String brokerTwoBootstrap = "127.0.0.1:" + brokerTwoPort;
        String clusterBootstrap =
                brokerOneBootstrap + "," + brokerTwoBootstrap;

        createBucket(bucket);
        writeConfiguration(
                brokerOneConfig,
                brokerOnePort,
                controllerPort,
                bucket,
                root.resolve("multi-broker-one-log"),
                root.resolve("multi-broker-one-metadata"),
                root.resolve("multi-broker-one-cache"),
                "OBJECT_WAL_SYNC_OBJECT",
                null,
                1,
                true,
                nereusCluster);
        writeConfiguration(
                brokerTwoConfig,
                brokerTwoPort,
                controllerPort,
                bucket,
                root.resolve("multi-broker-two-log"),
                root.resolve("multi-broker-two-metadata"),
                root.resolve("multi-broker-two-cache"),
                "OBJECT_WAL_SYNC_OBJECT",
                null,
                2,
                false,
                nereusCluster);
        formatStorage(
                formatScript,
                kafkaHome,
                brokerOneConfig,
                brokerOneFormatLog,
                kafkaClusterId);
        formatStorage(
                formatScript,
                kafkaHome,
                brokerTwoConfig,
                brokerTwoFormatLog,
                kafkaClusterId);

        TopicPartition partition = new TopicPartition(topic, 0);
        byte[] firstKey =
                "takeover-key-0".getBytes(StandardCharsets.UTF_8);
        byte[] firstValue =
                "nereus-process-takeover-first".getBytes(StandardCharsets.UTF_8);
        byte[] secondKey =
                "takeover-key-1".getBytes(StandardCharsets.UTF_8);
        byte[] secondValue =
                "nereus-process-takeover-second".getBytes(StandardCharsets.UTF_8);
        Process brokerOne = start(
                List.of(startScript.toString(), brokerOneConfig.toString()),
                kafkaHome,
                brokerOneServerLog);
        Process brokerTwo = null;
        Throwable failure = null;
        try {
            awaitBroker(brokerOneBootstrap, brokerOne, brokerOneServerLog);
            try (Admin admin =
                    Admin.create(adminProperties(brokerOneBootstrap))) {
                admin.createTopics(List.of(
                                new NewTopic(
                                        topic,
                                        Map.of(0, List.of(1)))))
                        .all()
                        .get(
                                CLIENT_TIMEOUT.toSeconds(),
                                TimeUnit.SECONDS);
            }
            RecordMetadata first = produce(
                    brokerOneBootstrap,
                    topic,
                    firstKey,
                    firstValue);
            assertThat(first.offset()).isZero();
            assertThat(fetch(
                            brokerOneBootstrap,
                            partition,
                            0,
                            brokerOneServerLog)
                    .value())
                    .isEqualTo(firstValue);

            brokerTwo = start(
                    List.of(startScript.toString(), brokerTwoConfig.toString()),
                    kafkaHome,
                    brokerTwoServerLog);
            awaitBroker(brokerTwoBootstrap, brokerTwo, brokerTwoServerLog);
            awaitClusterBrokers(
                    clusterBootstrap,
                    List.of(1, 2),
                    List.of(brokerOne, brokerTwo),
                    brokerOneServerLog,
                    brokerTwoServerLog);

            try (Admin admin =
                    Admin.create(adminProperties(clusterBootstrap))) {
                admin.alterPartitionReassignments(Map.of(
                                partition,
                                Optional.of(new NewPartitionReassignment(
                                        List.of(2)))))
                        .all()
                        .get(
                                CLIENT_TIMEOUT.toSeconds(),
                                TimeUnit.SECONDS);
                awaitPartitionLeader(
                        admin,
                        partition,
                        2,
                        brokerOne,
                        brokerTwo,
                        brokerOneServerLog,
                        brokerTwoServerLog);
                assertThat(admin.listPartitionReassignments(Set.of(partition))
                                .reassignments()
                                .get(
                                        CLIENT_TIMEOUT.toSeconds(),
                                        TimeUnit.SECONDS))
                        .as("Nereus RF1 handoff must not leave a stock catch-up reassignment")
                        .isEmpty();
            }

            assertThat(brokerOne.isAlive())
                    .as("the old Kafka process remains live during higher-leader-epoch takeover")
                    .isTrue();
            RecordMetadata second = produce(
                    clusterBootstrap,
                    topic,
                    secondKey,
                    secondValue);
            assertThat(second.offset()).isEqualTo(1L);
            ConsumerRecord<byte[], byte[]> recovered = fetch(
                    clusterBootstrap,
                    partition,
                    0,
                    brokerTwoServerLog);
            assertThat(recovered.key()).isEqualTo(firstKey);
            assertThat(recovered.value()).isEqualTo(firstValue);
            ConsumerRecord<byte[], byte[]> appended = fetch(
                    clusterBootstrap,
                    partition,
                    1,
                    brokerTwoServerLog);
            assertThat(appended.key()).isEqualTo(secondKey);
            assertThat(appended.value()).isEqualTo(secondValue);
        } catch (Exception | AssertionError operationFailure) {
            failure = operationFailure;
        }
        if (brokerTwo != null) {
            try {
                stopBroker(brokerTwo, brokerTwoServerLog);
            } catch (Exception | AssertionError shutdownFailure) {
                failure = mergeFailure(failure, shutdownFailure);
            }
        }
        try {
            stopBroker(brokerOne, brokerOneServerLog);
        } catch (Exception | AssertionError shutdownFailure) {
            failure = mergeFailure(failure, shutdownFailure);
        }
        if (failure != null) {
            try {
                preserveMultiBrokerFailureEvidence(
                        brokerOneConfig,
                        brokerTwoConfig,
                        brokerOneFormatLog,
                        brokerTwoFormatLog,
                        brokerOneServerLog,
                        brokerTwoServerLog);
            } catch (AssertionError evidenceFailure) {
                failure.addSuppressed(evidenceFailure);
            }
            rethrow(failure);
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void threeCombinedNodesKeepNativeIoThroughControllerLeaderKill()
            throws Exception {
        clearFailureEvidence();
        Path kafkaCheckout = requiredKafkaCheckout();
        Path kafkaHome =
                extractReleaseDistribution(
                        kafkaCheckout,
                        root.resolve("kafka-multi-controller-distribution"));
        Path formatScript =
                executable(kafkaHome.resolve("bin/kafka-storage.sh"));
        Path startScript =
                executable(
                        kafkaHome.resolve(
                                "bin/nereus-kafka-server-start.sh"));
        Path[] configs = new Path[3];
        Path[] formatLogs = new Path[3];
        Path[] serverLogs = new Path[3];
        int[] brokerPorts = new int[3];
        int[] controllerPorts = new int[3];
        List<Integer> allocatedPorts = new ArrayList<>();
        for (int index = 0; index < 3; index++) {
            int nodeId = index + 1;
            configs[index] =
                    root.resolve(
                            "multi-controller-node-"
                                    + nodeId
                                    + ".properties");
            formatLogs[index] =
                    root.resolve(
                            "multi-controller-node-"
                                    + nodeId
                                    + "-format.log");
            serverLogs[index] =
                    root.resolve(
                            "multi-controller-node-"
                                    + nodeId
                                    + "-server.log");
            brokerPorts[index] =
                    differentFreePort(
                            allocatedPorts.stream()
                                    .mapToInt(Integer::intValue)
                                    .toArray());
            allocatedPorts.add(brokerPorts[index]);
            controllerPorts[index] =
                    differentFreePort(
                            allocatedPorts.stream()
                                    .mapToInt(Integer::intValue)
                                    .toArray());
            allocatedPorts.add(controllerPorts[index]);
        }
        String controllerQuorumVoters =
                "1@127.0.0.1:"
                        + controllerPorts[0]
                        + ",2@127.0.0.1:"
                        + controllerPorts[1]
                        + ",3@127.0.0.1:"
                        + controllerPorts[2];
        List<String> bootstraps =
                List.of(
                        "127.0.0.1:" + brokerPorts[0],
                        "127.0.0.1:" + brokerPorts[1],
                        "127.0.0.1:" + brokerPorts[2]);
        String clusterBootstrap = String.join(",", bootstraps);
        String bucket =
                "nereus-kafka-ctrl-"
                        + UUID.randomUUID();
        String topic =
                "controller-failover-" + UUID.randomUUID();
        String nereusCluster =
                "f9-controller-failover-" + UUID.randomUUID();
        String kafkaClusterId =
                org.apache.kafka.common.Uuid.randomUuid().toString();
        createBucket(bucket);
        for (int index = 0; index < 3; index++) {
            int nodeId = index + 1;
            writeConfiguration(
                    configs[index],
                    brokerPorts[index],
                    controllerPorts[index],
                    bucket,
                    root.resolve(
                            "multi-controller-node-"
                                    + nodeId
                                    + "-log"),
                    root.resolve(
                            "multi-controller-node-"
                                    + nodeId
                                    + "-metadata"),
                    root.resolve(
                            "multi-controller-node-"
                                    + nodeId
                                    + "-cache"),
                    "OBJECT_WAL_SYNC_OBJECT",
                    null,
                    nodeId,
                    true,
                    controllerQuorumVoters,
                    nereusCluster,
                    LOCALSTACK
                            .getEndpointOverride(
                                    LocalStackContainer.Service.S3)
                            .toString());
            formatStorage(
                    formatScript,
                    kafkaHome,
                    configs[index],
                    formatLogs[index],
                    kafkaClusterId);
        }

        Process[] nodes = new Process[3];
        Throwable failure = null;
        try {
            for (int index = 0; index < 3; index++) {
                nodes[index] =
                        start(
                                List.of(
                                        startScript.toString(),
                                        configs[index].toString()),
                                kafkaHome,
                                serverLogs[index]);
            }
            for (int index = 0; index < 3; index++) {
                awaitBroker(
                        bootstraps.get(index),
                        nodes[index],
                        serverLogs[index]);
            }
            awaitClusterBrokers(
                    clusterBootstrap,
                    List.of(1, 2, 3),
                    List.of(nodes),
                    serverLogs);
            ControllerQuorumEvidence initialQuorum =
                    awaitControllerQuorum(
                            clusterBootstrap,
                            List.of(1, 2, 3),
                            -1,
                            -1,
                            List.of(nodes),
                            serverLogs);
            awaitControllerActivationReconciliation(
                    nodes[initialQuorum.leaderId() - 1],
                    serverLogs[initialQuorum.leaderId() - 1],
                    initialQuorum);
            KafkaActivationEvidence initialActivation =
                    awaitActiveActivation(
                            nereusCluster,
                            kafkaClusterId,
                            List.of(1, 2, 3),
                            serverLogs,
                            Duration.ofSeconds(30));

            int dataNodeId =
                    initialQuorum.leaderId() % 3 + 1;
            TopicPartition partition =
                    new TopicPartition(topic, 0);
            try (Admin admin =
                    Admin.create(
                            adminProperties(clusterBootstrap))) {
                admin.createTopics(
                                List.of(
                                        new NewTopic(
                                                topic,
                                                Map.of(
                                                        0,
                                                        List.of(
                                                                dataNodeId)))))
                        .all()
                        .get(
                                CLIENT_TIMEOUT.toSeconds(),
                                TimeUnit.SECONDS);
            }
            byte[] firstValue =
                    "controller-failover-before"
                            .getBytes(StandardCharsets.UTF_8);
            RecordMetadata first =
                    produce(
                            clusterBootstrap,
                            topic,
                            "controller-before"
                                    .getBytes(StandardCharsets.UTF_8),
                            firstValue);
            assertThat(first.offset()).isZero();
            assertThat(
                            fetch(
                                            clusterBootstrap,
                                            partition,
                                            0,
                                            serverLogs[
                                                    dataNodeId - 1])
                                    .value())
                    .isEqualTo(firstValue);

            Process oldController =
                    nodes[initialQuorum.leaderId() - 1];
            killBroker(
                    oldController,
                    serverLogs[initialQuorum.leaderId() - 1]);
            List<Process> survivors = new ArrayList<>();
            List<Path> survivorLogs = new ArrayList<>();
            List<String> survivorBootstraps =
                    new ArrayList<>();
            for (int index = 0; index < 3; index++) {
                if (index == initialQuorum.leaderId() - 1) {
                    continue;
                }
                survivors.add(nodes[index]);
                survivorLogs.add(serverLogs[index]);
                survivorBootstraps.add(bootstraps.get(index));
            }
            String survivingBootstrap =
                    String.join(",", survivorBootstraps);
            ControllerQuorumEvidence replacementQuorum =
                    awaitControllerQuorum(
                            survivingBootstrap,
                            List.of(1, 2, 3),
                            initialQuorum.leaderId(),
                            initialQuorum.leaderEpoch(),
                            survivors,
                            survivorLogs.toArray(Path[]::new));
            awaitControllerActivationReconciliation(
                    nodes[replacementQuorum.leaderId() - 1],
                    serverLogs[
                            replacementQuorum.leaderId() - 1],
                    replacementQuorum);
            KafkaActivationEvidence replacementActivation =
                    awaitActiveActivation(
                            nereusCluster,
                            kafkaClusterId,
                            List.of(1, 2, 3),
                            survivorLogs.toArray(Path[]::new),
                            Duration.ofSeconds(30));
            assertThat(replacementActivation.activation())
                    .as(
                            "controller failover must not rewrite the one-way ACTIVE authority")
                    .isEqualTo(
                            initialActivation.activation());
            assertThat(
                            replacementActivation
                                    .readiness()
                                    .readinessEpoch())
                    .isGreaterThanOrEqualTo(
                            initialActivation
                                    .readiness()
                                    .readinessEpoch());

            byte[] secondValue =
                    "controller-failover-after"
                            .getBytes(StandardCharsets.UTF_8);
            RecordMetadata second =
                    produce(
                            survivingBootstrap,
                            topic,
                            "controller-after"
                                    .getBytes(StandardCharsets.UTF_8),
                            secondValue);
            assertThat(second.offset()).isEqualTo(1L);
            assertThat(
                            fetch(
                                            survivingBootstrap,
                                            partition,
                                            0,
                                            serverLogs[
                                                    dataNodeId - 1])
                                    .value())
                    .isEqualTo(firstValue);
            assertThat(
                            fetch(
                                            survivingBootstrap,
                                            partition,
                                            1,
                                            serverLogs[
                                                    dataNodeId - 1])
                                    .value())
                    .isEqualTo(secondValue);
            try (Admin admin =
                    Admin.create(
                            adminProperties(
                                    survivingBootstrap))) {
                assertOffsets(admin, partition, 0, 2);
            }
            assertThat(objectCount(bucket)).isPositive();
        } catch (Throwable operationFailure) {
            failure = operationFailure;
        }
        for (int index = nodes.length - 1;
                index >= 0;
                index--) {
            Process node = nodes[index];
            if (node == null || !node.isAlive()) {
                continue;
            }
            try {
                killBroker(
                        node,
                        serverLogs[index]);
            } catch (Throwable shutdownFailure) {
                failure =
                        mergeFailure(
                                failure,
                                shutdownFailure);
            }
        }
        if (failure != null) {
            try {
                preserveMultiControllerFailureEvidence(
                        configs,
                        formatLogs,
                        serverLogs);
            } catch (AssertionError evidenceFailure) {
                failure.addSuppressed(evidenceFailure);
            }
            rethrow(failure);
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void threeControllersRecoverEveryActivationPublicationCut()
            throws Exception {
        clearFailureEvidence();
        Path kafkaCheckout = requiredKafkaCheckout();
        Path kafkaHome =
                extractReleaseDistribution(
                        kafkaCheckout,
                        root.resolve(
                                "kafka-activation-cut-distribution"));
        Path formatScript =
                executable(
                        kafkaHome.resolve(
                                "bin/kafka-storage.sh"));
        Path startScript =
                executable(
                        kafkaHome.resolve(
                                "bin/nereus-kafka-server-start.sh"));
        Path activationAgent =
                requiredActivationFaultAgent();
        for (ActivationPublicationCut cut :
                ActivationPublicationCut.values()) {
            runActivationPublicationCut(
                    kafkaHome,
                    formatScript,
                    startScript,
                    activationAgent,
                    cut);
        }
    }

    private void runActivationPublicationCut(
            Path kafkaHome,
            Path formatScript,
            Path startScript,
            Path activationAgent,
            ActivationPublicationCut cut
    ) throws Exception {
        int processCount = 4;
        int controllerCount = 3;
        String prefix =
                "activation-cut-" + cut.slug();
        Path[] configs = new Path[processCount];
        Path[] formatLogs = new Path[processCount];
        Path[] serverLogs = new Path[processCount];
        int[] brokerPorts = new int[processCount];
        int[] controllerPorts = new int[processCount];
        List<Integer> allocatedPorts = new ArrayList<>();
        for (int index = 0;
                index < processCount;
                index++) {
            int nodeId = index + 1;
            configs[index] =
                    root.resolve(
                            prefix
                                    + "-node-"
                                    + nodeId
                                    + ".properties");
            formatLogs[index] =
                    root.resolve(
                            prefix
                                    + "-node-"
                                    + nodeId
                                    + "-format.log");
            serverLogs[index] =
                    root.resolve(
                            prefix
                                    + "-node-"
                                    + nodeId
                                    + "-server.log");
            brokerPorts[index] =
                    differentFreePort(
                            allocatedPorts.stream()
                                    .mapToInt(Integer::intValue)
                                    .toArray());
            allocatedPorts.add(
                    brokerPorts[index]);
            controllerPorts[index] =
                    differentFreePort(
                            allocatedPorts.stream()
                                    .mapToInt(Integer::intValue)
                                    .toArray());
            allocatedPorts.add(
                    controllerPorts[index]);
        }
        String controllerQuorumVoters =
                "1@127.0.0.1:"
                        + controllerPorts[0]
                        + ",2@127.0.0.1:"
                        + controllerPorts[1]
                        + ",3@127.0.0.1:"
                        + controllerPorts[2];
        String controllerBootstrap =
                "127.0.0.1:"
                        + controllerPorts[0]
                        + ",127.0.0.1:"
                        + controllerPorts[1]
                        + ",127.0.0.1:"
                        + controllerPorts[2];
        String brokerBootstrap =
                "127.0.0.1:" + brokerPorts[3];
        String bucket =
                "nereus-act-"
                        + cut.slug()
                        + "-"
                        + UUID.randomUUID();
        String topic =
                prefix + "-" + UUID.randomUUID();
        String nereusCluster =
                "f9-"
                        + prefix
                        + "-"
                        + UUID.randomUUID();
        String kafkaClusterId =
                org.apache.kafka.common.Uuid
                        .randomUuid()
                        .toString();
        createBucket(bucket);
        for (int index = 0;
                index < processCount;
                index++) {
            int nodeId = index + 1;
            KafkaProcessRole role =
                    index < controllerCount
                            ? KafkaProcessRole.CONTROLLER
                            : KafkaProcessRole.BROKER;
            writeConfiguration(
                    configs[index],
                    brokerPorts[index],
                    controllerPorts[index],
                    bucket,
                    root.resolve(
                            prefix
                                    + "-node-"
                                    + nodeId
                                    + "-log"),
                    root.resolve(
                            prefix
                                    + "-node-"
                                    + nodeId
                                    + "-metadata"),
                    root.resolve(
                            prefix
                                    + "-node-"
                                    + nodeId
                                    + "-cache"),
                    "OBJECT_WAL_SYNC_OBJECT",
                    null,
                    nodeId,
                    role,
                    controllerQuorumVoters,
                    nereusCluster,
                    LOCALSTACK
                            .getEndpointOverride(
                                    LocalStackContainer.Service.S3)
                            .toString());
            formatStorage(
                    formatScript,
                    kafkaHome,
                    configs[index],
                    formatLogs[index],
                    kafkaClusterId);
        }

        ActivationAgentMarkers[] markers =
                new ActivationAgentMarkers[
                        controllerCount];
        for (int index = 0;
                index < controllerCount;
                index++) {
            int nodeId = index + 1;
            markers[index] =
                    new ActivationAgentMarkers(
                            root.resolve(
                                    prefix
                                            + "-controller-"
                                            + nodeId
                                            + "-agent-arm"),
                            root.resolve(
                                    prefix
                                            + "-controller-"
                                            + nodeId
                                            + "-agent-captured"),
                            root.resolve(
                                    prefix
                                            + "-controller-"
                                            + nodeId
                                            + "-agent-blocked"),
                            root.resolve(
                                    prefix
                                            + "-controller-"
                                            + nodeId
                                            + "-agent-applied"),
                            root.resolve(
                                    prefix
                                            + "-controller-"
                                            + nodeId
                                            + "-agent-installed"));
        }

        Process[] nodes =
                new Process[processCount];
        Throwable failure = null;
        try {
            for (int index = 0;
                    index < controllerCount;
                    index++) {
                nodes[index] =
                        start(
                                List.of(
                                        startScript.toString(),
                                        configs[index].toString()),
                                kafkaHome,
                                serverLogs[index],
                                Map.of(
                                        "KAFKA_OPTS",
                                        activationFaultAgentOptions(
                                                activationAgent,
                                                cut,
                                                markers[index])));
            }
            for (int index = 0;
                    index < controllerCount;
                    index++) {
                awaitMarker(
                        markers[index].installed(),
                        nodes[index],
                        serverLogs[index],
                        Duration.ofSeconds(30));
            }
            ControllerQuorumEvidence initialQuorum =
                    awaitControllerQuorum(
                            controllerAdminProperties(
                                    controllerBootstrap),
                            List.of(1, 2, 3),
                            -1,
                            -1,
                            List.of(
                                    nodes[0],
                                    nodes[1],
                                    nodes[2]),
                            serverLogs[0],
                            serverLogs[1],
                            serverLogs[2]);
            int gatedControllerIndex =
                    initialQuorum.leaderId() - 1;
            Files.createFile(
                    markers[gatedControllerIndex]
                            .arm());
            nodes[3] =
                    start(
                            List.of(
                                    startScript.toString(),
                                    configs[3].toString()),
                            kafkaHome,
                            serverLogs[3]);
            awaitActivationCutMarker(
                    cut,
                    markers,
                    gatedControllerIndex,
                    nodes,
                    serverLogs,
                    Duration.ofSeconds(45));
            assertThat(
                            Files.exists(
                                    markers[
                                                    gatedControllerIndex]
                                    .captured()))
                    .isTrue();
            KafkaActivationCutEvidence cutEvidence =
                    awaitActivationCutEvidence(
                            nereusCluster,
                            kafkaClusterId,
                            List.of(4),
                            cut,
                            serverLogs,
                            Duration.ofSeconds(30));
            assertThat(
                            readLog(
                                    serverLogs[
                                            gatedControllerIndex]))
                    .as(
                            "the killed controller must not observe the gated activation completion")
                    .doesNotContain(
                            activationReconciliationMarker(
                                    initialQuorum));

            killBroker(
                    nodes[gatedControllerIndex],
                    serverLogs[
                            gatedControllerIndex]);
            List<Process> survivors =
                    new ArrayList<>();
            List<Path> survivorLogs =
                    new ArrayList<>();
            for (int index = 0;
                    index < processCount;
                    index++) {
                if (index
                        == gatedControllerIndex) {
                    continue;
                }
                survivors.add(nodes[index]);
                survivorLogs.add(
                        serverLogs[index]);
            }
            awaitBroker(
                    brokerBootstrap,
                    nodes[3],
                    serverLogs[3]);
            ControllerQuorumEvidence replacementQuorum =
                    awaitControllerQuorum(
                            brokerBootstrap,
                            List.of(1, 2, 3),
                            initialQuorum.leaderId(),
                            initialQuorum.leaderEpoch(),
                            survivors,
                            survivorLogs.toArray(
                                    Path[]::new));
            awaitControllerActivationReconciliation(
                    nodes[
                            replacementQuorum.leaderId()
                                    - 1],
                    serverLogs[
                            replacementQuorum.leaderId()
                                    - 1],
                    replacementQuorum);
            KafkaActivationEvidence recovered =
                    awaitActiveActivation(
                            nereusCluster,
                            kafkaClusterId,
                            List.of(4),
                            survivorLogs.toArray(
                                    Path[]::new),
                            Duration.ofSeconds(30));
            if (cut.durableState()
                    == ActivationDurableState
                            .PREPARED) {
                assertPreparedFactsPreserved(
                        cutEvidence
                                .activation()
                                .orElseThrow(),
                        recovered.activation());
            } else if (cut.durableState()
                    == ActivationDurableState
                            .ACTIVE) {
                assertThat(recovered.activation())
                        .as(
                                "an applied ACTIVE response loss must not rewrite activation")
                        .isEqualTo(
                                cutEvidence
                                        .activation()
                                        .orElseThrow());
            } else {
                assertThat(
                                cutEvidence
                                        .activation())
                        .as(
                                "a before-provider PREPARED cut must not create activation")
                        .isEmpty();
            }
            assertThat(
                            recovered
                                    .readiness()
                                    .readinessEpoch())
                    .isGreaterThanOrEqualTo(
                            cutEvidence
                                    .readiness()
                                    .readinessEpoch());

            awaitClusterBrokers(
                    brokerBootstrap,
                    List.of(4),
                    survivors,
                    survivorLogs.toArray(
                            Path[]::new));
            try (Admin admin =
                    Admin.create(
                            adminProperties(
                                    brokerBootstrap))) {
                admin.createTopics(
                                List.of(
                                        new NewTopic(
                                                topic,
                                                Map.of(
                                                        0,
                                                        List.of(4)))))
                        .all()
                        .get(
                                CLIENT_TIMEOUT.toSeconds(),
                                TimeUnit.SECONDS);
            }
            byte[] value =
                    ("activation-cut-"
                                    + cut.slug())
                            .getBytes(
                                    StandardCharsets.UTF_8);
            RecordMetadata produced =
                    produce(
                            brokerBootstrap,
                            topic,
                            ("activation-key-"
                                            + cut.slug())
                                    .getBytes(
                                            StandardCharsets.UTF_8),
                            value);
            assertThat(produced.offset())
                    .isZero();
            TopicPartition partition =
                    new TopicPartition(
                            topic,
                            0);
            assertThat(
                            fetch(
                                            brokerBootstrap,
                                            partition,
                                            0,
                                            serverLogs[3])
                                    .value())
                    .isEqualTo(value);
            try (Admin admin =
                    Admin.create(
                            adminProperties(
                                    brokerBootstrap))) {
                assertOffsets(
                        admin,
                        partition,
                        0,
                        1);
            }
            assertThat(objectCount(bucket))
                    .isPositive();
        } catch (Throwable operationFailure) {
            failure = operationFailure;
        }
        for (int index = nodes.length - 1;
                index >= 0;
                index--) {
            Process node = nodes[index];
            if (node == null || !node.isAlive()) {
                continue;
            }
            try {
                killBroker(
                        node,
                        serverLogs[index]);
            } catch (Throwable shutdownFailure) {
                failure =
                        mergeFailure(
                                failure,
                                shutdownFailure);
            }
        }
        if (failure != null) {
            try {
                preserveActivationCutFailureEvidence(
                        cut,
                        configs,
                        formatLogs,
                        serverLogs,
                        markers);
            } catch (AssertionError evidenceFailure) {
                failure.addSuppressed(
                        evidenceFailure);
            }
            rethrow(failure);
        }
    }

    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void threeReleaseProcessesFenceAlreadyDispatchedOldLeaderAppend()
            throws Exception {
        clearFailureEvidence();
        Path kafkaCheckout = requiredKafkaCheckout();
        Path kafkaHome = extractReleaseDistribution(
                kafkaCheckout,
                root.resolve("kafka-inflight-takeover-distribution"));
        Path formatScript = executable(kafkaHome.resolve("bin/kafka-storage.sh"));
        Path startScript = executable(kafkaHome.resolve("bin/nereus-kafka-server-start.sh"));
        Path controllerConfig = root.resolve("inflight-controller.properties");
        Path brokerOneConfig = root.resolve("inflight-broker-one.properties");
        Path brokerTwoConfig = root.resolve("inflight-broker-two.properties");
        Path controllerFormatLog = root.resolve("inflight-controller-format.log");
        Path brokerOneFormatLog = root.resolve("inflight-broker-one-format.log");
        Path brokerTwoFormatLog = root.resolve("inflight-broker-two-format.log");
        Path controllerServerLog = root.resolve("inflight-controller-server.log");
        Path brokerOneServerLog = root.resolve("inflight-broker-one-server.log");
        Path brokerTwoServerLog = root.resolve("inflight-broker-two-server.log");
        String bucket = "nereus-kafka-inflight-" + UUID.randomUUID();
        String topic = "process-inflight-" + UUID.randomUUID();
        String nereusCluster = "f9-process-inflight-" + UUID.randomUUID();
        String kafkaClusterId =
                org.apache.kafka.common.Uuid.randomUuid().toString();
        int controllerBrokerPort = freePort();
        int controllerPort = differentFreePort(controllerBrokerPort);
        int brokerOnePort =
                differentFreePort(controllerBrokerPort, controllerPort);
        int brokerTwoPort =
                differentFreePort(
                        controllerBrokerPort,
                        controllerPort,
                        brokerOnePort);
        String controllerBootstrap = "127.0.0.1:" + controllerBrokerPort;
        String brokerOneBootstrap = "127.0.0.1:" + brokerOnePort;
        String brokerTwoBootstrap = "127.0.0.1:" + brokerTwoPort;
        String clusterBootstrap =
                brokerOneBootstrap + "," + brokerTwoBootstrap + "," + controllerBootstrap;
        String toxicName = "hold-old-wal-response";

        createBucket(bucket);
        org.testcontainers.Testcontainers.exposeHostPorts(LOCALSTACK.getMappedPort(4566));
        try (ToxiproxyContainer toxiproxy =
                new ToxiproxyContainer(TOXIPROXY_IMAGE)) {
            toxiproxy.start();
            ToxiproxyContainer.ContainerProxy objectProxy =
                    toxiproxy.getProxy(
                            "host.testcontainers.internal",
                            LOCALSTACK.getMappedPort(4566));
            String proxiedObjectEndpoint =
                    "http://"
                            + objectProxy.getContainerIpAddress()
                            + ":"
                            + objectProxy.getProxyPort();

            writeConfiguration(
                    controllerConfig,
                    controllerBrokerPort,
                    controllerPort,
                    bucket,
                    root.resolve("inflight-controller-log"),
                    root.resolve("inflight-controller-metadata"),
                    root.resolve("inflight-controller-cache"),
                    "OBJECT_WAL_SYNC_OBJECT",
                    null,
                    3,
                    true,
                    3,
                    nereusCluster,
                    proxiedObjectEndpoint);
            writeConfiguration(
                    brokerOneConfig,
                    brokerOnePort,
                    controllerPort,
                    bucket,
                    root.resolve("inflight-broker-one-log"),
                    root.resolve("inflight-broker-one-metadata"),
                    root.resolve("inflight-broker-one-cache"),
                    "OBJECT_WAL_SYNC_OBJECT",
                    null,
                    1,
                    false,
                    3,
                    nereusCluster,
                    proxiedObjectEndpoint);
            writeConfiguration(
                    brokerTwoConfig,
                    brokerTwoPort,
                    controllerPort,
                    bucket,
                    root.resolve("inflight-broker-two-log"),
                    root.resolve("inflight-broker-two-metadata"),
                    root.resolve("inflight-broker-two-cache"),
                    "OBJECT_WAL_SYNC_OBJECT",
                    null,
                    2,
                    false,
                    3,
                    nereusCluster,
                    proxiedObjectEndpoint);
            formatStorage(
                    formatScript,
                    kafkaHome,
                    controllerConfig,
                    controllerFormatLog,
                    kafkaClusterId);
            formatStorage(
                    formatScript,
                    kafkaHome,
                    brokerOneConfig,
                    brokerOneFormatLog,
                    kafkaClusterId);
            formatStorage(
                    formatScript,
                    kafkaHome,
                    brokerTwoConfig,
                    brokerTwoFormatLog,
                    kafkaClusterId);

            TopicPartition partition = new TopicPartition(topic, 0);
            byte[] committedKey =
                    "inflight-key-0".getBytes(StandardCharsets.UTF_8);
            byte[] committedValue =
                    "nereus-inflight-committed".getBytes(StandardCharsets.UTF_8);
            byte[] staleKey =
                    "inflight-stale-key".getBytes(StandardCharsets.UTF_8);
            byte[] staleValue =
                    "nereus-inflight-old-leader".getBytes(StandardCharsets.UTF_8);
            byte[] currentKey =
                    "inflight-current-key".getBytes(StandardCharsets.UTF_8);
            byte[] currentValue =
                    "nereus-inflight-current-leader".getBytes(StandardCharsets.UTF_8);

            Process controller = null;
            Process brokerOne = null;
            Process brokerTwo = null;
            PendingProduce staleProduce = null;
            boolean brokerOnePaused = false;
            boolean toxicInstalled = false;
            Throwable failure = null;
            try {
                controller =
                        start(
                                List.of(
                                        startScript.toString(),
                                        controllerConfig.toString()),
                                kafkaHome,
                                controllerServerLog);
                awaitBroker(
                        controllerBootstrap,
                        controller,
                        controllerServerLog);
                brokerOne =
                        start(
                                List.of(
                                        startScript.toString(),
                                        brokerOneConfig.toString()),
                                kafkaHome,
                                brokerOneServerLog);
                brokerTwo =
                        start(
                                List.of(
                                        startScript.toString(),
                                        brokerTwoConfig.toString()),
                                kafkaHome,
                                brokerTwoServerLog);
                awaitBroker(
                        brokerOneBootstrap,
                        brokerOne,
                        brokerOneServerLog);
                awaitBroker(
                        brokerTwoBootstrap,
                        brokerTwo,
                        brokerTwoServerLog);
                awaitClusterBrokers(
                        clusterBootstrap,
                        List.of(1, 2, 3),
                        List.of(controller, brokerOne, brokerTwo),
                        controllerServerLog,
                        brokerOneServerLog,
                        brokerTwoServerLog);

                try (Admin admin =
                        Admin.create(adminProperties(
                                brokerTwoBootstrap + "," + controllerBootstrap))) {
                    admin.createTopics(List.of(
                                    new NewTopic(
                                            topic,
                                            Map.of(0, List.of(1)))))
                            .all()
                            .get(
                                    CLIENT_TIMEOUT.toSeconds(),
                                    TimeUnit.SECONDS);
                }
                RecordMetadata committed =
                        produce(
                                brokerOneBootstrap,
                                topic,
                                committedKey,
                                committedValue);
                assertThat(committed.offset()).isZero();
                assertThat(fetch(
                                        brokerOneBootstrap,
                                        partition,
                                        0,
                                        brokerOneServerLog)
                                .value())
                        .isEqualTo(committedValue);

                Set<String> baselineWalObjects = walObjectKeys(bucket);
                objectProxy
                        .toxics()
                        .timeout(
                                toxicName,
                                ToxicDirection.DOWNSTREAM,
                                0);
                toxicInstalled = true;
                staleProduce =
                        beginSingleAttemptProduce(
                                brokerOneBootstrap,
                                topic,
                                staleKey,
                                staleValue);
                awaitProviderAppendStack(
                        brokerOne,
                        staleProduce,
                        brokerOneServerLog);
                assertThat(staleProduce.future().isDone())
                        .as("the old Produce remains inside provider IO while its WAL response is held")
                        .isFalse();

                signalProcess(
                        brokerOne,
                        "STOP",
                        brokerOneServerLog);
                brokerOnePaused = true;
                removeToxic(objectProxy, toxicName);
                toxicInstalled = false;

                try (Admin admin =
                        Admin.create(adminProperties(
                                brokerTwoBootstrap + "," + controllerBootstrap))) {
                    admin.alterPartitionReassignments(Map.of(
                                    partition,
                                    Optional.of(new NewPartitionReassignment(
                                            List.of(2)))))
                            .all()
                            .get(
                                    CLIENT_TIMEOUT.toSeconds(),
                                    TimeUnit.SECONDS);
                    awaitPartitionLeader(
                            admin,
                            partition,
                            2,
                            brokerOne,
                            brokerTwo,
                            brokerOneServerLog,
                            brokerTwoServerLog);
                    assertThat(admin.listPartitionReassignments(Set.of(partition))
                                    .reassignments()
                                    .get(
                                            CLIENT_TIMEOUT.toSeconds(),
                                            TimeUnit.SECONDS))
                            .isEmpty();
                    assertOffsets(admin, partition, 0, 1);
                }
                ConsumerRecord<byte[], byte[]> recovered =
                        fetch(
                                brokerTwoBootstrap,
                                partition,
                                0,
                                brokerTwoServerLog);
                assertThat(recovered.key()).isEqualTo(committedKey);
                assertThat(recovered.value()).isEqualTo(committedValue);

                signalProcess(
                        brokerOne,
                        "CONT",
                        brokerOneServerLog);
                brokerOnePaused = false;
                Throwable staleFailure = staleProduce.awaitFailure();
                assertThat(staleFailure)
                        .as("the already-dispatched old-leader append must fail after authority takeover")
                        .isNotNull();
                assertThat(readLog(brokerOneServerLog))
                        .contains("append session changed before guarded object upload");
                assertThat(walObjectKeys(bucket))
                        .as("pre-upload fencing must not leave an orphan WAL object")
                        .isEqualTo(baselineWalObjects);
                assertThat(brokerOne.isAlive())
                        .as("the old process must survive its stale append completion")
                        .isTrue();
                try (Admin admin =
                        Admin.create(adminProperties(clusterBootstrap))) {
                    assertOffsets(admin, partition, 0, 1);
                }

                RecordMetadata current =
                        produce(
                                clusterBootstrap,
                                topic,
                                currentKey,
                                currentValue);
                assertThat(current.offset()).isEqualTo(1L);
                ConsumerRecord<byte[], byte[]> appended =
                        fetch(
                                brokerTwoBootstrap,
                                partition,
                                1,
                                brokerTwoServerLog);
                assertThat(appended.key()).isEqualTo(currentKey);
                assertThat(appended.value()).isEqualTo(currentValue);
                try (Admin admin =
                        Admin.create(adminProperties(clusterBootstrap))) {
                    assertOffsets(admin, partition, 0, 2);
                }
            } catch (Throwable operationFailure) {
                failure = operationFailure;
            }
            if (toxicInstalled) {
                try {
                    removeToxic(objectProxy, toxicName);
                } catch (Throwable cleanupFailure) {
                    failure = mergeFailure(failure, cleanupFailure);
                }
            }
            if (brokerOnePaused && brokerOne != null && brokerOne.isAlive()) {
                try {
                    signalProcess(
                            brokerOne,
                            "CONT",
                            brokerOneServerLog);
                } catch (Throwable cleanupFailure) {
                    failure = mergeFailure(failure, cleanupFailure);
                }
            }
            if (staleProduce != null) {
                staleProduce.close();
            }
            if (brokerTwo != null) {
                try {
                    stopBroker(brokerTwo, brokerTwoServerLog);
                } catch (Throwable cleanupFailure) {
                    failure = mergeFailure(failure, cleanupFailure);
                }
            }
            if (brokerOne != null) {
                try {
                    stopBroker(brokerOne, brokerOneServerLog);
                } catch (Throwable cleanupFailure) {
                    failure = mergeFailure(failure, cleanupFailure);
                }
            }
            if (controller != null) {
                try {
                    stopBroker(controller, controllerServerLog);
                } catch (Throwable cleanupFailure) {
                    failure = mergeFailure(failure, cleanupFailure);
                }
            }
            if (failure != null) {
                try {
                    preserveInFlightTakeoverFailureEvidence(
                            controllerConfig,
                            brokerOneConfig,
                            brokerTwoConfig,
                            controllerFormatLog,
                            brokerOneFormatLog,
                            brokerTwoFormatLog,
                            controllerServerLog,
                            brokerOneServerLog,
                            brokerTwoServerLog);
                } catch (AssertionError evidenceFailure) {
                    failure.addSuppressed(evidenceFailure);
                }
                rethrow(failure);
            }
        }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    void threeBookKeeperProfilesAtomicallyReassignLiveSharedStorageLeader()
            throws Exception {
        clearFailureEvidence();
        Path kafkaCheckout = requiredKafkaCheckout();
        Path kafkaHome =
                extractReleaseDistribution(
                        kafkaCheckout,
                        root.resolve("kafka-bookkeeper-takeover-distribution"));
        Path formatScript =
                executable(kafkaHome.resolve("bin/kafka-storage.sh"));
        Path startScript =
                executable(kafkaHome.resolve("bin/nereus-kafka-server-start.sh"));
        int zooKeeperPort = freePort();
        String metadataServiceUri =
                "zk+longhierarchical://127.0.0.1:"
                        + zooKeeperPort
                        + "/ledgers";

        try (LocalBookKeeper ignored = startBookKeeper(zooKeeperPort)) {
            for (BookKeeperTakeoverProfile profile :
                    List.of(
                            new BookKeeperTakeoverProfile(
                                    "BOOKKEEPER_WAL_ONLY",
                                    "wal-only",
                                    4,
                                    false),
                            new BookKeeperTakeoverProfile(
                                    "BOOKKEEPER_WAL_ASYNC_OBJECT",
                                    "wal-async-object",
                                    5,
                                    true),
                            new BookKeeperTakeoverProfile(
                                    "BOOKKEEPER_WAL_SYNC_OBJECT",
                                    "wal-sync-object",
                                    6,
                                    true))) {
                runBookKeeperProfileLiveTakeover(
                        formatScript,
                        startScript,
                        kafkaHome,
                        metadataServiceUri,
                        profile);
            }
        }
    }

    private void runBookKeeperProfileLiveTakeover(
            Path formatScript,
            Path startScript,
            Path kafkaHome,
            String metadataServiceUri,
            BookKeeperTakeoverProfile profile
    ) throws Exception {
        String fixtureToken = "bookkeeper-takeover-" + profile.fixtureToken();
        Path brokerOneConfig =
                root.resolve(fixtureToken + "-one.properties");
        Path brokerTwoConfig =
                root.resolve(fixtureToken + "-two.properties");
        Path brokerOneFormatLog =
                root.resolve(fixtureToken + "-one-format.log");
        Path brokerTwoFormatLog =
                root.resolve(fixtureToken + "-two-format.log");
        Path brokerOneServerLog =
                root.resolve(fixtureToken + "-one-server.log");
        Path brokerTwoServerLog =
                root.resolve(fixtureToken + "-two-server.log");
        Path passwordFile =
                root.resolve(fixtureToken + "-password.bin");
        Files.write(
                passwordFile,
                ("f9-" + fixtureToken + "-process-password")
                        .getBytes(StandardCharsets.UTF_8));

        String bucket =
                "nereus-kafka-bk-takeover-"
                        + profile.authoritySeed()
                        + "-"
                        + UUID.randomUUID();
        String topic =
                fixtureToken + "-process-gate-" + UUID.randomUUID();
        String nereusCluster =
                "f9-" + fixtureToken + "-" + UUID.randomUUID();
        String kafkaClusterId =
                org.apache.kafka.common.Uuid.randomUuid().toString();
        int brokerOnePort = freePort();
        int controllerPort = differentFreePort(brokerOnePort);
        int brokerTwoPort =
                differentFreePort(brokerOnePort, controllerPort);
        String brokerOneBootstrap = "127.0.0.1:" + brokerOnePort;
        String brokerTwoBootstrap = "127.0.0.1:" + brokerTwoPort;
        String clusterBootstrap =
                brokerOneBootstrap + "," + brokerTwoBootstrap;
        BookKeeperProcessConfiguration bookKeeper =
                bookKeeperProcessConfiguration(
                        metadataServiceUri,
                        fixtureToken,
                        passwordFile,
                        profile.authoritySeed(),
                        2);
        seedBookKeeperAuthority(
                oxiaConfiguration(),
                bookKeeperWalConfiguration(
                        bookKeeper,
                        profile.storageProfile()
                                .equals(
                                        "BOOKKEEPER_WAL_ASYNC_OBJECT")),
                bookKeeper,
                Clock.systemUTC());
        createBucket(bucket);
        writeConfiguration(
                brokerOneConfig,
                brokerOnePort,
                controllerPort,
                bucket,
                root.resolve(fixtureToken + "-one-log"),
                root.resolve(fixtureToken + "-one-metadata"),
                root.resolve(fixtureToken + "-one-cache"),
                profile.storageProfile(),
                bookKeeper,
                1,
                true,
                nereusCluster);
        writeConfiguration(
                brokerTwoConfig,
                brokerTwoPort,
                controllerPort,
                bucket,
                root.resolve(fixtureToken + "-two-log"),
                root.resolve(fixtureToken + "-two-metadata"),
                root.resolve(fixtureToken + "-two-cache"),
                profile.storageProfile(),
                bookKeeper,
                2,
                false,
                nereusCluster);
        formatStorage(
                formatScript,
                kafkaHome,
                brokerOneConfig,
                brokerOneFormatLog,
                kafkaClusterId);
        formatStorage(
                formatScript,
                kafkaHome,
                brokerTwoConfig,
                brokerTwoFormatLog,
                kafkaClusterId);

        TopicPartition partition = new TopicPartition(topic, 0);
        byte[] firstKey =
                (fixtureToken + "-key-0")
                        .getBytes(StandardCharsets.UTF_8);
        byte[] firstValue =
                (fixtureToken + "-first")
                        .getBytes(StandardCharsets.UTF_8);
        byte[] secondKey =
                (fixtureToken + "-key-1")
                        .getBytes(StandardCharsets.UTF_8);
        byte[] secondValue =
                (fixtureToken + "-second")
                        .getBytes(StandardCharsets.UTF_8);
        Process brokerOne =
                start(
                        List.of(
                                startScript.toString(),
                                brokerOneConfig.toString()),
                        kafkaHome,
                        brokerOneServerLog);
        Process brokerTwo =
                start(
                        List.of(
                                startScript.toString(),
                                brokerTwoConfig.toString()),
                        kafkaHome,
                        brokerTwoServerLog);
        Throwable failure = null;
        try {
            awaitBroker(
                    brokerOneBootstrap,
                    brokerOne,
                    brokerOneServerLog);
            awaitBroker(
                    brokerTwoBootstrap,
                    brokerTwo,
                    brokerTwoServerLog);
            awaitClusterBrokers(
                    clusterBootstrap,
                    List.of(1, 2),
                    List.of(brokerOne, brokerTwo),
                    brokerOneServerLog,
                    brokerTwoServerLog);
            try (Admin admin =
                    Admin.create(adminProperties(clusterBootstrap))) {
                admin.createTopics(
                                List.of(
                                        new NewTopic(
                                                topic,
                                                Map.of(0, List.of(1)))))
                        .all()
                        .get(
                                CLIENT_TIMEOUT.toSeconds(),
                                TimeUnit.SECONDS);
            }
            RecordMetadata first =
                    produce(
                            brokerOneBootstrap,
                            topic,
                            firstKey,
                            firstValue);
            assertThat(first.offset()).isZero();
            assertThat(
                            fetch(
                                            brokerOneBootstrap,
                                            partition,
                                            0,
                                            brokerOneServerLog)
                                    .value())
                    .isEqualTo(firstValue);
            assertBookKeeperProfileObjects(
                    profile,
                    bucket,
                    "before takeover");

            try (Admin admin =
                    Admin.create(adminProperties(clusterBootstrap))) {
                admin.alterPartitionReassignments(
                                Map.of(
                                        partition,
                                        Optional.of(
                                                new NewPartitionReassignment(
                                                        List.of(2)))))
                        .all()
                        .get(
                                CLIENT_TIMEOUT.toSeconds(),
                                TimeUnit.SECONDS);
                awaitPartitionLeader(
                        admin,
                        partition,
                        2,
                        brokerOne,
                        brokerTwo,
                        brokerOneServerLog,
                        brokerTwoServerLog);
                assertThat(
                                admin.listPartitionReassignments(
                                                Set.of(partition))
                                        .reassignments()
                                        .get(
                                                CLIENT_TIMEOUT.toSeconds(),
                                                TimeUnit.SECONDS))
                        .as(
                                profile.storageProfile()
                                        + " handoff must not retain a stock catch-up reassignment")
                        .isEmpty();
                assertOffsets(admin, partition, 0, 1);
            }
            assertThat(brokerOne.isAlive())
                    .as(
                            profile.storageProfile()
                                    + " old process remains live after higher-epoch takeover")
                    .isTrue();
            ConsumerRecord<byte[], byte[]> recovered =
                    fetch(
                            brokerTwoBootstrap,
                            partition,
                            0,
                            brokerTwoServerLog);
            assertThat(recovered.key()).isEqualTo(firstKey);
            assertThat(recovered.value()).isEqualTo(firstValue);

            RecordMetadata second =
                    produce(
                            brokerTwoBootstrap,
                            topic,
                            secondKey,
                            secondValue);
            assertThat(second.offset()).isEqualTo(1L);
            ConsumerRecord<byte[], byte[]> appended =
                    fetch(
                            brokerTwoBootstrap,
                            partition,
                            1,
                            brokerTwoServerLog);
            assertThat(appended.key()).isEqualTo(secondKey);
            assertThat(appended.value()).isEqualTo(secondValue);
            try (Admin admin =
                    Admin.create(adminProperties(clusterBootstrap))) {
                assertOffsets(admin, partition, 0, 2);
            }
            assertBookKeeperProfileObjects(
                    profile,
                    bucket,
                    "after takeover continuation");
        } catch (Throwable operationFailure) {
            failure = operationFailure;
        }
        try {
            stopBroker(brokerTwo, brokerTwoServerLog);
        } catch (Throwable shutdownFailure) {
            failure = mergeFailure(failure, shutdownFailure);
        }
        try {
            stopBroker(brokerOne, brokerOneServerLog);
        } catch (Throwable shutdownFailure) {
            failure = mergeFailure(failure, shutdownFailure);
        }
        if (failure != null) {
            try {
                preserveBookKeeperTakeoverFailureEvidence(
                        profile.fixtureToken(),
                        brokerOneConfig,
                        brokerTwoConfig,
                        brokerOneFormatLog,
                        brokerTwoFormatLog,
                        brokerOneServerLog,
                        brokerTwoServerLog);
            } catch (AssertionError evidenceFailure) {
                failure.addSuppressed(evidenceFailure);
            }
            rethrow(failure);
        }
    }

    private static void assertBookKeeperProfileObjects(
            BookKeeperTakeoverProfile profile,
            String bucket,
            String phase
    ) throws InterruptedException {
        if (profile.requireMaterializedObject()) {
            awaitPositiveObjectCount(bucket);
            return;
        }
        assertThat(objectCount(bucket))
                .as(
                        profile.storageProfile()
                                + " must remain BookKeeper-only "
                                + phase)
                .isZero();
    }

    @Test
    @Timeout(value = 8, unit = TimeUnit.MINUTES)
    void threeReleaseProcessesFenceAppliedBookKeeperWriteBeforePublication()
            throws Exception {
        clearFailureEvidence();
        Path kafkaCheckout = requiredKafkaCheckout();
        Path kafkaHome =
                extractReleaseDistribution(
                        kafkaCheckout,
                        root.resolve(
                                "kafka-bookkeeper-inflight-distribution"));
        Path formatScript =
                executable(kafkaHome.resolve("bin/kafka-storage.sh"));
        Path startScript =
                executable(
                        kafkaHome.resolve(
                                "bin/nereus-kafka-server-start.sh"));
        Path faultAgent = requiredBookKeeperFaultAgent();
        Path controllerConfig =
                root.resolve("bookkeeper-inflight-controller.properties");
        Path brokerOneConfig =
                root.resolve("bookkeeper-inflight-broker-one.properties");
        Path brokerTwoConfig =
                root.resolve("bookkeeper-inflight-broker-two.properties");
        Path controllerFormatLog =
                root.resolve("bookkeeper-inflight-controller-format.log");
        Path brokerOneFormatLog =
                root.resolve("bookkeeper-inflight-broker-one-format.log");
        Path brokerTwoFormatLog =
                root.resolve("bookkeeper-inflight-broker-two-format.log");
        Path controllerServerLog =
                root.resolve("bookkeeper-inflight-controller-server.log");
        Path brokerOneServerLog =
                root.resolve("bookkeeper-inflight-broker-one-server.log");
        Path brokerTwoServerLog =
                root.resolve("bookkeeper-inflight-broker-two-server.log");
        Path passwordFile =
                root.resolve("bookkeeper-inflight-password.bin");
        Path agentArm = root.resolve("bookkeeper-inflight-agent-arm");
        Path agentCaptured =
                root.resolve("bookkeeper-inflight-agent-captured");
        Path agentApplied =
                root.resolve("bookkeeper-inflight-agent-applied");
        Path agentRelease =
                root.resolve("bookkeeper-inflight-agent-release");
        Path agentInstalled =
                root.resolve("bookkeeper-inflight-agent-installed");
        Files.write(
                passwordFile,
                "f9-bookkeeper-inflight-process-password"
                        .getBytes(StandardCharsets.UTF_8));

        String bucket =
                "nereus-kafka-bk-inflight-" + UUID.randomUUID();
        String topic =
                "bookkeeper-inflight-process-" + UUID.randomUUID();
        String nereusCluster =
                "f9-bookkeeper-inflight-" + UUID.randomUUID();
        String kafkaClusterId =
                org.apache.kafka.common.Uuid.randomUuid().toString();
        int controllerBrokerPort = freePort();
        int controllerPort =
                differentFreePort(controllerBrokerPort);
        int brokerOnePort =
                differentFreePort(
                        controllerBrokerPort,
                        controllerPort);
        int brokerTwoPort =
                differentFreePort(
                        controllerBrokerPort,
                        controllerPort,
                        brokerOnePort);
        int zooKeeperPort =
                differentFreePort(
                        controllerBrokerPort,
                        controllerPort,
                        brokerOnePort,
                        brokerTwoPort);
        String controllerBootstrap =
                "127.0.0.1:" + controllerBrokerPort;
        String brokerOneBootstrap =
                "127.0.0.1:" + brokerOnePort;
        String brokerTwoBootstrap =
                "127.0.0.1:" + brokerTwoPort;
        String clusterBootstrap =
                brokerOneBootstrap
                        + ","
                        + brokerTwoBootstrap
                        + ","
                        + controllerBootstrap;
        String metadataServiceUri =
                "zk+longhierarchical://127.0.0.1:"
                        + zooKeeperPort
                        + "/ledgers";
        BookKeeperProcessConfiguration bookKeeper =
                bookKeeperProcessConfiguration(
                        metadataServiceUri,
                        "bookkeeper-inflight",
                        passwordFile,
                        7,
                        3);
        BookKeeperWalConfiguration bookKeeperWal =
                bookKeeperWalConfiguration(bookKeeper, false);
        seedBookKeeperAuthority(
                oxiaConfiguration(),
                bookKeeperWal,
                bookKeeper,
                Clock.systemUTC());
        createBucket(bucket);
        writeConfiguration(
                controllerConfig,
                controllerBrokerPort,
                controllerPort,
                bucket,
                root.resolve("bookkeeper-inflight-controller-log"),
                root.resolve(
                        "bookkeeper-inflight-controller-metadata"),
                root.resolve(
                        "bookkeeper-inflight-controller-cache"),
                "BOOKKEEPER_WAL_ONLY",
                bookKeeper,
                3,
                true,
                3,
                nereusCluster,
                LOCALSTACK
                        .getEndpointOverride(
                                LocalStackContainer.Service.S3)
                        .toString());
        writeConfiguration(
                brokerOneConfig,
                brokerOnePort,
                controllerPort,
                bucket,
                root.resolve("bookkeeper-inflight-broker-one-log"),
                root.resolve(
                        "bookkeeper-inflight-broker-one-metadata"),
                root.resolve(
                        "bookkeeper-inflight-broker-one-cache"),
                "BOOKKEEPER_WAL_ONLY",
                bookKeeper,
                1,
                false,
                3,
                nereusCluster,
                LOCALSTACK
                        .getEndpointOverride(
                                LocalStackContainer.Service.S3)
                        .toString());
        writeConfiguration(
                brokerTwoConfig,
                brokerTwoPort,
                controllerPort,
                bucket,
                root.resolve("bookkeeper-inflight-broker-two-log"),
                root.resolve(
                        "bookkeeper-inflight-broker-two-metadata"),
                root.resolve(
                        "bookkeeper-inflight-broker-two-cache"),
                "BOOKKEEPER_WAL_ONLY",
                bookKeeper,
                2,
                false,
                3,
                nereusCluster,
                LOCALSTACK
                        .getEndpointOverride(
                                LocalStackContainer.Service.S3)
                        .toString());
        formatStorage(
                formatScript,
                kafkaHome,
                controllerConfig,
                controllerFormatLog,
                kafkaClusterId);
        formatStorage(
                formatScript,
                kafkaHome,
                brokerOneConfig,
                brokerOneFormatLog,
                kafkaClusterId);
        formatStorage(
                formatScript,
                kafkaHome,
                brokerTwoConfig,
                brokerTwoFormatLog,
                kafkaClusterId);

        String agentOptions =
                bookKeeperFaultAgentOptions(
                        faultAgent,
                        agentArm,
                        agentCaptured,
                        agentApplied,
                        agentRelease,
                        agentInstalled);
        TopicPartition partition = new TopicPartition(topic, 0);
        byte[] committedKey =
                "bookkeeper-inflight-key-0"
                        .getBytes(StandardCharsets.UTF_8);
        byte[] committedValue =
                "bookkeeper-inflight-committed"
                        .getBytes(StandardCharsets.UTF_8);
        byte[] staleKey =
                "bookkeeper-inflight-stale-key"
                        .getBytes(StandardCharsets.UTF_8);
        byte[] staleValue =
                "bookkeeper-inflight-stale-value"
                        .getBytes(StandardCharsets.UTF_8);
        byte[] currentKey =
                "bookkeeper-inflight-current-key"
                        .getBytes(StandardCharsets.UTF_8);
        byte[] currentValue =
                "bookkeeper-inflight-current-value"
                        .getBytes(StandardCharsets.UTF_8);

        try (LocalBookKeeper ignored =
                        startBookKeeper(zooKeeperPort);
                BookKeeper inspector =
                        bookKeeperClient(metadataServiceUri)) {
            Process controller = null;
            Process brokerOne = null;
            Process brokerTwo = null;
            PendingProduce staleProduce = null;
            boolean brokerOnePaused = false;
            boolean releasePublished = false;
            Throwable failure = null;
            try {
                controller =
                        start(
                                List.of(
                                        startScript.toString(),
                                        controllerConfig.toString()),
                                kafkaHome,
                                controllerServerLog);
                awaitBroker(
                        controllerBootstrap,
                        controller,
                        controllerServerLog);
                brokerOne =
                        start(
                                List.of(
                                        startScript.toString(),
                                        brokerOneConfig.toString()),
                                kafkaHome,
                                brokerOneServerLog,
                                Map.of("KAFKA_OPTS", agentOptions));
                brokerTwo =
                        start(
                                List.of(
                                        startScript.toString(),
                                        brokerTwoConfig.toString()),
                                kafkaHome,
                                brokerTwoServerLog);
                awaitBroker(
                        brokerOneBootstrap,
                        brokerOne,
                        brokerOneServerLog);
                awaitBroker(
                        brokerTwoBootstrap,
                        brokerTwo,
                        brokerTwoServerLog);
                awaitMarker(
                        agentInstalled,
                        brokerOne,
                        brokerOneServerLog,
                        Duration.ofSeconds(30));
                awaitClusterBrokers(
                        clusterBootstrap,
                        List.of(1, 2, 3),
                        List.of(controller, brokerOne, brokerTwo),
                        controllerServerLog,
                        brokerOneServerLog,
                        brokerTwoServerLog);
                try (Admin admin =
                        Admin.create(
                                adminProperties(
                                        brokerTwoBootstrap
                                                + ","
                                                + controllerBootstrap))) {
                    admin.createTopics(
                                    List.of(
                                            new NewTopic(
                                                    topic,
                                                    Map.of(
                                                            0,
                                                            List.of(
                                                                    1)))))
                            .all()
                            .get(
                                    CLIENT_TIMEOUT.toSeconds(),
                                    TimeUnit.SECONDS);
                }
                RecordMetadata committed =
                        produce(
                                brokerOneBootstrap,
                                topic,
                                committedKey,
                                committedValue);
                assertThat(committed.offset()).isZero();
                assertThat(
                                fetch(
                                                brokerOneBootstrap,
                                                partition,
                                                0,
                                                brokerOneServerLog)
                                        .value())
                        .isEqualTo(committedValue);
                KafkaPartitionId partitionId =
                        kafkaPartitionId(
                                clusterBootstrap,
                                partition,
                                topic);
                Files.createFile(agentArm);
                staleProduce =
                        beginSingleAttemptProduce(
                                brokerOneBootstrap,
                                topic,
                                staleKey,
                                staleValue);
                String appliedMarker =
                        awaitAppliedMarker(
                                agentApplied,
                                brokerOne,
                                staleProduce,
                                brokerOneServerLog,
                                Duration.ofSeconds(45));
                assertThat(Files.exists(agentCaptured)).isTrue();
                long appliedEntryId =
                        Long.parseLong(appliedMarker.strip());
                awaitProviderAppendStack(
                        brokerOne,
                        staleProduce,
                        brokerOneServerLog);
                BookKeeperInFlightEvidence inFlight =
                        awaitBookKeeperWritingReservation(
                                nereusCluster,
                                partitionId,
                                bookKeeperWal,
                                appliedEntryId,
                                brokerOneServerLog,
                                Duration.ofSeconds(30));
                assertPhysicalBookKeeperEntry(
                        inspector,
                        inFlight.ledgerId(),
                        inFlight.entryId(),
                        Files.readAllBytes(passwordFile));
                assertThat(staleProduce.future().isDone())
                        .as(
                                "the old Produce remains pending after the Bookie ack")
                        .isFalse();
                try (Admin admin =
                        Admin.create(
                                adminProperties(clusterBootstrap))) {
                    assertOffsets(admin, partition, 0, 1);
                }

                signalProcess(
                        brokerOne,
                        "STOP",
                        brokerOneServerLog);
                brokerOnePaused = true;
                try (Admin admin =
                        Admin.create(
                                adminProperties(
                                        brokerTwoBootstrap
                                                + ","
                                                + controllerBootstrap))) {
                    admin.alterPartitionReassignments(
                                    Map.of(
                                            partition,
                                            Optional.of(
                                                    new NewPartitionReassignment(
                                                            List.of(
                                                                    2)))))
                            .all()
                            .get(
                                    CLIENT_TIMEOUT.toSeconds(),
                                    TimeUnit.SECONDS);
                    awaitPartitionLeader(
                            admin,
                            partition,
                            2,
                            brokerOne,
                            brokerTwo,
                            brokerOneServerLog,
                            brokerTwoServerLog);
                    assertThat(
                                    admin.listPartitionReassignments(
                                                    Set.of(
                                                            partition))
                                            .reassignments()
                                            .get(
                                                    CLIENT_TIMEOUT
                                                            .toSeconds(),
                                                    TimeUnit.SECONDS))
                            .isEmpty();
                    assertOffsets(admin, partition, 0, 1);
                }
                ConsumerRecord<byte[], byte[]> recovered =
                        fetch(
                                brokerTwoBootstrap,
                                partition,
                                0,
                                brokerTwoServerLog);
                assertThat(recovered.key()).isEqualTo(committedKey);
                assertThat(recovered.value())
                        .isEqualTo(committedValue);
                RecordMetadata current =
                        produce(
                                brokerTwoBootstrap,
                                topic,
                                currentKey,
                                currentValue);
                assertThat(current.offset()).isEqualTo(1L);
                ConsumerRecord<byte[], byte[]> appended =
                        fetch(
                                brokerTwoBootstrap,
                                partition,
                                1,
                                brokerTwoServerLog);
                assertThat(appended.key()).isEqualTo(currentKey);
                assertThat(appended.value())
                        .isEqualTo(currentValue);
                awaitBookKeeperTakeoverReconciliation(
                        nereusCluster,
                        bookKeeperWal,
                        inFlight,
                        brokerTwoServerLog,
                        Duration.ofSeconds(30));
                assertThat(controller.isAlive()).isTrue();

                Files.createFile(agentRelease);
                releasePublished = true;
                signalProcess(
                        brokerOne,
                        "CONT",
                        brokerOneServerLog);
                brokerOnePaused = false;
                Throwable staleFailure =
                        staleProduce.awaitFailure();
                assertThat(staleFailure)
                        .as(
                                "the provider-applied old BookKeeper append must fail after takeover")
                        .isNotNull();
                assertThat(brokerOne.isAlive())
                        .as(
                                "the old process survives stale BookKeeper completion")
                        .isTrue();
                try (Admin admin =
                        Admin.create(
                                adminProperties(clusterBootstrap))) {
                    assertOffsets(admin, partition, 0, 2);
                }
                assertThat(objectCount(bucket))
                        .as(
                                "BookKeeper WAL-only takeover must not publish Object bytes")
                        .isZero();
                awaitBookKeeperTakeoverReconciliation(
                        nereusCluster,
                        bookKeeperWal,
                        inFlight,
                        brokerTwoServerLog,
                        Duration.ofSeconds(30));
            } catch (Throwable operationFailure) {
                failure = operationFailure;
            }
            if (!releasePublished) {
                try {
                    Files.writeString(agentRelease, "release");
                } catch (Throwable cleanupFailure) {
                    failure =
                            mergeFailure(
                                    failure,
                                    cleanupFailure);
                }
            }
            if (brokerOnePaused
                    && brokerOne != null
                    && brokerOne.isAlive()) {
                try {
                    signalProcess(
                            brokerOne,
                            "CONT",
                            brokerOneServerLog);
                } catch (Throwable cleanupFailure) {
                    failure =
                            mergeFailure(
                                    failure,
                                    cleanupFailure);
                }
            }
            if (staleProduce != null) {
                staleProduce.close();
            }
            if (brokerTwo != null) {
                try {
                    stopBroker(
                            brokerTwo,
                            brokerTwoServerLog);
                } catch (Throwable cleanupFailure) {
                    failure =
                            mergeFailure(
                                    failure,
                                    cleanupFailure);
                }
            }
            if (brokerOne != null) {
                try {
                    stopBroker(
                            brokerOne,
                            brokerOneServerLog);
                } catch (Throwable cleanupFailure) {
                    failure =
                            mergeFailure(
                                    failure,
                                    cleanupFailure);
                }
            }
            if (controller != null) {
                try {
                    stopBroker(
                            controller,
                            controllerServerLog);
                } catch (Throwable cleanupFailure) {
                    failure =
                            mergeFailure(
                                    failure,
                                    cleanupFailure);
                }
            }
            if (failure != null) {
                try {
                    preserveBookKeeperInFlightFailureEvidence(
                            controllerConfig,
                            brokerOneConfig,
                            brokerTwoConfig,
                            controllerFormatLog,
                            brokerOneFormatLog,
                            brokerTwoFormatLog,
                            controllerServerLog,
                            brokerOneServerLog,
                            brokerTwoServerLog,
                            agentInstalled,
                            agentCaptured,
                            agentApplied,
                            agentRelease);
                } catch (AssertionError evidenceFailure) {
                    failure.addSuppressed(evidenceFailure);
                }
                rethrow(failure);
            }
        }
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void objectWalAsyncObjectProcessRecoversAcrossFreshJvmRestart()
            throws Exception {
        clearFailureEvidence();
        Path kafkaCheckout = requiredKafkaCheckout();
        Path kafkaHome =
                extractReleaseDistribution(
                        kafkaCheckout,
                        root.resolve("kafka-object-async-distribution"));
        Path formatScript =
                executable(kafkaHome.resolve("bin/kafka-storage.sh"));
        Path startScript =
                executable(kafkaHome.resolve("bin/nereus-kafka-server-start.sh"));
        Path config = root.resolve("object-async-server.properties");
        Path formatLog = root.resolve("object-async-format.log");
        Path firstServerLog = root.resolve("object-async-server-first.log");
        Path restartServerLog =
                root.resolve("object-async-server-restart.log");
        String bucket = "nereus-kafka-object-async-" + UUID.randomUUID();
        String topic = "object-async-process-gate-" + UUID.randomUUID();
        int brokerPort = freePort();
        int controllerPort = differentFreePort(brokerPort);
        String bootstrapServers = "127.0.0.1:" + brokerPort;

        createBucket(bucket);
        writeConfiguration(
                config,
                brokerPort,
                controllerPort,
                bucket,
                root.resolve("object-async-kafka-log"),
                root.resolve("object-async-metadata-log"),
                root.resolve("object-async-nereus-cache"),
                "OBJECT_WAL_ASYNC_OBJECT",
                null);
        try {
            runSimpleProfileColdRestart(
                    formatScript,
                    startScript,
                    kafkaHome,
                    config,
                    formatLog,
                    firstServerLog,
                    restartServerLog,
                    bootstrapServers,
                    topic,
                    "object-async");
            assertThat(objectCount(bucket))
                    .as("the async profile must retain its durable Object WAL across restart")
                    .isPositive();
        } catch (Exception | AssertionError failure) {
            try {
                preserveFailureEvidence(
                        config,
                        formatLog,
                        firstServerLog,
                        restartServerLog);
            } catch (AssertionError evidenceFailure) {
                failure.addSuppressed(evidenceFailure);
            }
            throw failure;
        }
    }

    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)
    void bookKeeperWalOnlyProcessRecoversAcrossFreshJvmRestart()
            throws Exception {
        runBookKeeperProfileColdRestart(
                "BOOKKEEPER_WAL_ONLY",
                "bookkeeper",
                1,
                false,
                1);
    }

    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)
    void bookKeeperWalAsyncObjectProcessMaterializesAndRecoversAcrossFreshJvmRestart()
            throws Exception {
        runBookKeeperProfileColdRestart(
                "BOOKKEEPER_WAL_ASYNC_OBJECT",
                "bookkeeper-async",
                4,
                true,
                2);
    }

    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)
    void bookKeeperWalSyncObjectProcessMaterializesBeforeAppendAndRecoversAcrossFreshJvmRestart()
            throws Exception {
        runBookKeeperProfileColdRestart(
                "BOOKKEEPER_WAL_SYNC_OBJECT",
                "bookkeeper-sync",
                1,
                true,
                3);
    }

    private void runBookKeeperProfileColdRestart(
            String storageProfile,
            String fixtureToken,
            int initialRecordCount,
            boolean requireMaterializedObject,
            int authoritySeed
    ) throws Exception {
        clearFailureEvidence();
        Path kafkaCheckout = requiredKafkaCheckout();
        Path kafkaHome = extractReleaseDistribution(
                kafkaCheckout,
                root.resolve("kafka-" + fixtureToken + "-distribution"));
        Path formatScript = executable(kafkaHome.resolve("bin/kafka-storage.sh"));
        Path startScript = executable(kafkaHome.resolve("bin/nereus-kafka-server-start.sh"));
        Path config = root.resolve(fixtureToken + "-server.properties");
        Path formatLog = root.resolve(fixtureToken + "-format.log");
        Path firstServerLog = root.resolve(fixtureToken + "-server-first.log");
        Path restartServerLog = root.resolve(fixtureToken + "-server-restart.log");
        Path passwordFile = root.resolve(fixtureToken + "-password.bin");
        Files.write(
                passwordFile,
                ("f9-" + fixtureToken + "-process-password")
                        .getBytes(StandardCharsets.UTF_8));

        String bucket =
                "nereus-kafka-bk" + authoritySeed + "-" + UUID.randomUUID();
        String topic = fixtureToken + "-process-gate-" + UUID.randomUUID();
        int brokerPort = freePort();
        int controllerPort = differentFreePort(brokerPort);
        String bootstrapServers = "127.0.0.1:" + brokerPort;
        createBucket(bucket);
        int zooKeeperPort = differentFreePort(controllerPort, brokerPort);
        String metadataServiceUri =
                "zk+longhierarchical://127.0.0.1:" + zooKeeperPort + "/ledgers";
        boolean requirePhysicalLedgerDeletion =
                storageProfile.equals("BOOKKEEPER_WAL_ASYNC_OBJECT");
        BookKeeperProcessConfiguration bookKeeper =
                bookKeeperProcessConfiguration(
                        metadataServiceUri,
                        fixtureToken,
                        passwordFile,
                        authoritySeed,
                        1);
        BookKeeperWalConfiguration bookKeeperWal =
                bookKeeperWalConfiguration(
                        bookKeeper,
                        requirePhysicalLedgerDeletion);
        seedBookKeeperAuthority(
                oxiaConfiguration(),
                bookKeeperWal,
                bookKeeper,
                Clock.systemUTC());
        try (LocalBookKeeper ignored = startBookKeeper(zooKeeperPort)) {
            String nereusCluster = writeConfiguration(
                    config,
                    brokerPort,
                    controllerPort,
                    bucket,
                    root.resolve(fixtureToken + "-kafka-log"),
                    root.resolve(fixtureToken + "-metadata-log"),
                    root.resolve(fixtureToken + "-nereus-cache"),
                    storageProfile,
                    bookKeeper);
            try (BookKeeper inspector =
                    requirePhysicalLedgerDeletion
                            ? bookKeeperClient(metadataServiceUri)
                            : null) {
                FirstBrokerAssertions firstBrokerAssertions =
                        requirePhysicalLedgerDeletion
                                ? (runningBootstrapServers,
                                        partition,
                                        runningTopic,
                                        serverLog) ->
                                        assertProcessLedgerDeletion(
                                                runningBootstrapServers,
                                                partition,
                                                runningTopic,
                                                serverLog,
                                                nereusCluster,
                                                bookKeeperWal,
                                                bookKeeper,
                                                inspector)
                                : FirstBrokerAssertions.NONE;
                runSimpleProfileColdRestart(
                        formatScript,
                        startScript,
                        kafkaHome,
                        config,
                        formatLog,
                        firstServerLog,
                        restartServerLog,
                        bootstrapServers,
                        topic,
                        fixtureToken,
                        initialRecordCount,
                        requireMaterializedObject
                                ? bucket
                                : null,
                        firstBrokerAssertions);
            }
        } catch (Exception | AssertionError failure) {
            try {
                preserveFailureEvidence(
                        config,
                        formatLog,
                        firstServerLog,
                        restartServerLog);
            } catch (AssertionError evidenceFailure) {
                failure.addSuppressed(evidenceFailure);
            }
            throw failure;
        }
    }

    private String writeConfiguration(
            Path config,
            int brokerPort,
            int controllerPort,
            String bucket,
            Path logDirectory,
            Path metadataDirectory,
            Path cacheDirectory
    ) throws IOException {
        return writeConfiguration(
                config,
                brokerPort,
                controllerPort,
                bucket,
                logDirectory,
                metadataDirectory,
                cacheDirectory,
                "OBJECT_WAL_SYNC_OBJECT",
                null);
    }

    private String writeConfiguration(
            Path config,
            int brokerPort,
            int controllerPort,
            String bucket,
            Path logDirectory,
            Path metadataDirectory,
            Path cacheDirectory,
            String storageProfile,
            BookKeeperProcessConfiguration bookKeeper
    ) throws IOException {
        return writeConfiguration(
                config,
                brokerPort,
                controllerPort,
                bucket,
                logDirectory,
                metadataDirectory,
                cacheDirectory,
                storageProfile,
                bookKeeper,
                1,
                true,
                "f9-process-" + UUID.randomUUID());
    }

    private String writeConfiguration(
            Path config,
            int brokerPort,
            int controllerPort,
            String bucket,
            Path logDirectory,
            Path metadataDirectory,
            Path cacheDirectory,
            String storageProfile,
            BookKeeperProcessConfiguration bookKeeper,
            int nodeId,
            boolean controllerRole,
            String nereusCluster
    ) throws IOException {
        return writeConfiguration(
                config,
                brokerPort,
                controllerPort,
                bucket,
                logDirectory,
                metadataDirectory,
                cacheDirectory,
                storageProfile,
                bookKeeper,
                nodeId,
                controllerRole,
                1,
                nereusCluster,
                LOCALSTACK
                        .getEndpointOverride(LocalStackContainer.Service.S3)
                        .toString());
    }

    private String writeConfiguration(
            Path config,
            int brokerPort,
            int controllerPort,
            String bucket,
            Path logDirectory,
            Path metadataDirectory,
            Path cacheDirectory,
            String storageProfile,
            BookKeeperProcessConfiguration bookKeeper,
            int nodeId,
            boolean controllerRole,
            int controllerNodeId,
            String nereusCluster,
            String objectEndpoint
    ) throws IOException {
        return writeConfiguration(
                config,
                brokerPort,
                controllerPort,
                bucket,
                logDirectory,
                metadataDirectory,
                cacheDirectory,
                storageProfile,
                bookKeeper,
                nodeId,
                controllerRole,
                controllerNodeId
                        + "@127.0.0.1:"
                        + controllerPort,
                nereusCluster,
                objectEndpoint);
    }

    private String writeConfiguration(
            Path config,
            int brokerPort,
            int controllerPort,
            String bucket,
            Path logDirectory,
            Path metadataDirectory,
            Path cacheDirectory,
            String storageProfile,
            BookKeeperProcessConfiguration bookKeeper,
            int nodeId,
            boolean controllerRole,
            String controllerQuorumVoters,
            String nereusCluster,
            String objectEndpoint
    ) throws IOException {
        return writeConfiguration(
                config,
                brokerPort,
                controllerPort,
                bucket,
                logDirectory,
                metadataDirectory,
                cacheDirectory,
                storageProfile,
                bookKeeper,
                nodeId,
                controllerRole
                        ? KafkaProcessRole.COMBINED
                        : KafkaProcessRole.BROKER,
                controllerQuorumVoters,
                nereusCluster,
                objectEndpoint);
    }

    private String writeConfiguration(
            Path config,
            int brokerPort,
            int controllerPort,
            String bucket,
            Path logDirectory,
            Path metadataDirectory,
            Path cacheDirectory,
            String storageProfile,
            BookKeeperProcessConfiguration bookKeeper,
            int nodeId,
            KafkaProcessRole processRole,
            String controllerQuorumVoters,
            String nereusCluster,
            String objectEndpoint
    ) throws IOException {
        boolean bookKeeperProfile = storageProfile.startsWith("BOOKKEEPER_WAL_");
        if (bookKeeperProfile != (bookKeeper != null)) {
            throw new IllegalArgumentException(
                    "BookKeeper process configuration must exactly match the selected storage profile");
        }
        if (nodeId <= 0) {
            throw new IllegalArgumentException("nodeId must be positive");
        }
        Objects.requireNonNull(processRole, "processRole");
        if (controllerQuorumVoters == null
                || controllerQuorumVoters.isBlank()) {
            throw new IllegalArgumentException(
                    "controllerQuorumVoters must be non-blank");
        }
        if (nereusCluster == null || nereusCluster.isBlank()) {
            throw new IllegalArgumentException("nereusCluster must be non-blank");
        }
        if (objectEndpoint == null || objectEndpoint.isBlank()) {
            throw new IllegalArgumentException("objectEndpoint must be non-blank");
        }
        Files.createDirectories(logDirectory);
        Files.createDirectories(metadataDirectory);
        Files.createDirectories(cacheDirectory);
        Properties properties = new Properties();
        properties.setProperty(
                "process.roles",
                processRole.configurationValue());
        properties.setProperty("node.id", Integer.toString(nodeId));
        properties.setProperty(
                "controller.quorum.voters",
                controllerQuorumVoters);
        properties.setProperty(
                "listeners",
                processRole.listeners(
                        brokerPort,
                        controllerPort));
        if (processRole.hasBroker()) {
            properties.setProperty(
                    "advertised.listeners",
                    "PLAINTEXT://127.0.0.1:" + brokerPort);
        }
        properties.setProperty("controller.listener.names", "CONTROLLER");
        properties.setProperty(
                "listener.security.protocol.map",
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT");
        properties.setProperty("inter.broker.listener.name", "PLAINTEXT");
        properties.setProperty("log.dirs", logDirectory.toString());
        properties.setProperty("metadata.log.dir", metadataDirectory.toString());
        properties.setProperty("num.partitions", "1");
        properties.setProperty("default.replication.factor", "1");
        properties.setProperty("min.insync.replicas", "1");
        properties.setProperty("offsets.topic.num.partitions", "1");
        properties.setProperty("offsets.topic.replication.factor", "1");
        properties.setProperty("group.initial.rebalance.delay.ms", "0");
        properties.setProperty("transaction.state.log.num.partitions", "1");
        properties.setProperty("transaction.state.log.replication.factor", "1");
        properties.setProperty("transaction.state.log.min.isr", "1");
        properties.setProperty("share.coordinator.state.topic.replication.factor", "1");
        properties.setProperty("share.coordinator.state.topic.min.isr", "1");
        properties.setProperty("auto.create.topics.enable", "false");
        properties.setProperty("remote.log.storage.system.enable", "false");
        properties.setProperty("log.cleaner.enable", "false");

        properties.setProperty("nereus.kafka.storage.enabled", "true");
        properties.setProperty("nereus.kafka.storage.cluster", nereusCluster);
        properties.setProperty(
                "nereus.kafka.storage.profile",
                storageProfile);
        properties.setProperty(
                "nereus.kafka.storage.oxia.service.address",
                OXIA.getServiceAddress());
        properties.setProperty("nereus.kafka.storage.oxia.namespace", "default");
        properties.setProperty("nereus.kafka.storage.object.provider", "s3");
        properties.setProperty("nereus.kafka.storage.object.bucket", bucket);
        properties.setProperty(
                "nereus.kafka.storage.object.endpoint",
                objectEndpoint);
        properties.setProperty(
                "nereus.kafka.storage.object.region",
                LOCALSTACK.getRegion());
        properties.setProperty("nereus.kafka.storage.object.path.style.access", "true");
        properties.setProperty("nereus.kafka.storage.cache.dir", cacheDirectory.toString());
        properties.setProperty("nereus.kafka.storage.compaction.enabled", "true");
        properties.setProperty("nereus.kafka.storage.append.executor.threads", "2");
        // Cold restart loads the user log plus both Kafka coordinator logs concurrently. Keep one additional
        // Fetch slot for the probe client while deliberately remaining well below the production default.
        properties.setProperty("nereus.kafka.storage.fetch.executor.threads", "4");
        properties.setProperty("nereus.kafka.storage.lifecycle.executor.threads", "2");
        properties.setProperty("nereus.kafka.storage.recovery.executor.threads", "2");
        properties.setProperty("nereus.kafka.storage.readiness.timeout.ms", "90000");
        properties.setProperty("nereus.kafka.storage.capability.heartbeat.ms", "1000");
        properties.setProperty("nereus.kafka.storage.capability.expiry.ms", "30000");
        properties.setProperty("nereus.kafka.storage.shutdown.drain.timeout.ms", "30000");
        properties.setProperty("nereus.kafka.storage.shutdown.checkpoint.timeout.ms", "30000");
        if (bookKeeper != null) {
            addBookKeeperConfiguration(
                    properties,
                    bookKeeper,
                    storageProfile.equals(
                            "BOOKKEEPER_WAL_ASYNC_OBJECT"));
        }
        try (Writer writer = Files.newBufferedWriter(config, StandardCharsets.UTF_8)) {
            properties.store(writer, "F9 native Kafka provider-backed process gate");
        }
        return nereusCluster;
    }

    private static void addBookKeeperConfiguration(
            Properties properties,
            BookKeeperProcessConfiguration bookKeeper,
            boolean enablePhysicalLedgerDeletion
    ) {
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.metadata.service.uri",
                bookKeeper.metadataServiceUri());
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.deployment.id",
                bookKeeper.deploymentId());
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.cluster.alias",
                bookKeeper.clusterAlias());
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.provider.scope.sha256",
                bookKeeper.providerScopeSha256());
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.ledger.id.prefix.bits",
                Integer.toString(bookKeeper.ledgerIdPrefixBits()));
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.ledger.id.prefix.value",
                Long.toString(bookKeeper.ledgerIdPrefixValue()));
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.ledger.id.reservation.id",
                bookKeeper.ledgerIdReservationId());
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.password.file",
                bookKeeper.passwordFile().toString());
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.password.version",
                bookKeeper.passwordVersion());
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.max.reads.inflight",
                "2");
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.readiness.epoch",
                Long.toString(bookKeeper.readinessEpoch()));
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.readiness.sha256",
                bookKeeper.readinessSha256());
        properties.setProperty(
                "nereus.kafka.storage.bookkeeper.persistent.broker.count",
                Integer.toString(bookKeeper.persistentBrokerCount()));
        if (enablePhysicalLedgerDeletion) {
            properties.setProperty(
                    "nereus.kafka.storage.fetch.executor.threads",
                    "8");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.max.reads.inflight",
                    "8");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.max.entries.per.ledger",
                    "1");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.operation.timeout.ms",
                    "4000");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.allocation.timeout.ms",
                    "4000");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.seal.timeout.ms",
                    "4000");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.delete.timeout.ms",
                    "4000");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.reader.lease.ttl.ms",
                    "5000");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.reader.lease.renew.ms",
                    "1000");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.retention.scan.interval.ms",
                    "1000");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.gc.enabled",
                    "true");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.gc.dry.run",
                    "false");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.gc.max.concurrent.deletes",
                    "1");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.gc.max.clock.skew.ms",
                    "0");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.gc.drain.grace.ms",
                    "5000");
            properties.setProperty(
                    "nereus.kafka.storage.bookkeeper.gc.late.create.audit.grace.ms",
                    "1000");
            properties.setProperty(
                    "nereus.kafka.storage.materialization.source.retirement.grace.ms",
                    "1000");
            properties.setProperty(
                    "nereus.kafka.storage.materialization.append.replay.grace.ms",
                    "1000");
            properties.setProperty(
                    "nereus.kafka.storage.materialization.metadata.audit.grace.ms",
                    "1000");
        }
    }

    private static void formatStorage(
            Path formatScript,
            Path kafkaHome,
            Path config,
            Path formatLog
    ) throws Exception {
        formatStorage(
                formatScript,
                kafkaHome,
                config,
                formatLog,
                org.apache.kafka.common.Uuid.randomUuid().toString());
    }

    private static void formatStorage(
            Path formatScript,
            Path kafkaHome,
            Path config,
            Path formatLog,
            String kafkaClusterId
    ) throws Exception {
        Process format = start(
                List.of(
                        formatScript.toString(),
                        "format",
                        "--cluster-id",
                        kafkaClusterId,
                        "--config",
                        config.toString(),
                        "--feature",
                        "nereus.storage.version=1"),
                kafkaHome,
                formatLog);
        int formatExit =
                await(format, PROCESS_TIMEOUT, "Kafka storage format", formatLog);
        assertThat(formatExit)
                .withFailMessage(() -> "storage format failed:\n" + readLog(formatLog))
                .isZero();
    }

    private static void runSimpleProfileColdRestart(
            Path formatScript,
            Path startScript,
            Path kafkaHome,
            Path config,
            Path formatLog,
            Path firstServerLog,
            Path restartServerLog,
            String bootstrapServers,
            String topic,
            String payloadPrefix
    ) throws Exception {
        runSimpleProfileColdRestart(
                formatScript,
                startScript,
                kafkaHome,
                config,
                formatLog,
                firstServerLog,
                restartServerLog,
                bootstrapServers,
                topic,
                payloadPrefix,
                1,
                null,
                FirstBrokerAssertions.NONE);
    }

    private static void runSimpleProfileColdRestart(
            Path formatScript,
            Path startScript,
            Path kafkaHome,
            Path config,
            Path formatLog,
            Path firstServerLog,
            Path restartServerLog,
            String bootstrapServers,
            String topic,
            String payloadPrefix,
            int initialRecordCount,
            String requiredMaterializationBucket,
            FirstBrokerAssertions firstBrokerAssertions
    ) throws Exception {
        if (initialRecordCount <= 0) {
            throw new IllegalArgumentException(
                    "initialRecordCount must be positive");
        }
        formatStorage(formatScript, kafkaHome, config, formatLog);
        TopicPartition partition = new TopicPartition(topic, 0);
        List<byte[]> initialKeys = new ArrayList<>(initialRecordCount);
        List<byte[]> initialValues = new ArrayList<>(initialRecordCount);
        for (int offset = 0; offset < initialRecordCount; offset++) {
            initialKeys.add(
                    (payloadPrefix + "-key-" + offset)
                            .getBytes(StandardCharsets.UTF_8));
            initialValues.add(
                    ("nereus-" + payloadPrefix + "-process-" + offset)
                            .getBytes(StandardCharsets.UTF_8));
        }
        runBroker(
                startScript,
                kafkaHome,
                config,
                formatLog,
                firstServerLog,
                bootstrapServers,
                () -> {
                    try (Admin admin =
                            Admin.create(adminProperties(bootstrapServers))) {
                        admin.createTopics(
                                        List.of(
                                                new NewTopic(
                                                        topic,
                                                        1,
                                                        (short) 1)))
                                .all()
                                .get(
                                        CLIENT_TIMEOUT.toSeconds(),
                                        TimeUnit.SECONDS);
                        for (int offset = 0;
                                offset < initialRecordCount;
                                offset++) {
                            RecordMetadata produced =
                                    produce(
                                            bootstrapServers,
                                            topic,
                                            initialKeys.get(offset),
                                            initialValues.get(offset));
                            assertThat(produced.partition()).isZero();
                            assertThat(produced.offset()).isEqualTo(offset);
                        }
                        ConsumerRecord<byte[], byte[]> fetched =
                                fetch(
                                        bootstrapServers,
                                        partition,
                                        initialRecordCount - 1L,
                                        firstServerLog);
                        assertThat(fetched.key())
                                .isEqualTo(
                                        initialKeys.get(
                                                initialRecordCount - 1));
                        assertThat(fetched.value())
                                .isEqualTo(
                                        initialValues.get(
                                                initialRecordCount - 1));
                        assertOffsets(
                                admin,
                                partition,
                                0,
                                initialRecordCount);
                        if (requiredMaterializationBucket != null) {
                            awaitPositiveObjectCount(
                                    requiredMaterializationBucket);
                        }
                        firstBrokerAssertions.verify(
                                bootstrapServers,
                                partition,
                                topic,
                                firstServerLog);
                    }
                });

        byte[] secondKey =
                (payloadPrefix + "-key-" + initialRecordCount)
                        .getBytes(StandardCharsets.UTF_8);
        byte[] secondValue =
                ("nereus-" + payloadPrefix + "-process-second")
                        .getBytes(StandardCharsets.UTF_8);
        runBroker(
                startScript,
                kafkaHome,
                config,
                formatLog,
                restartServerLog,
                bootstrapServers,
                () -> {
                    ConsumerRecord<byte[], byte[]> recovered =
                            fetch(
                                    bootstrapServers,
                                    partition,
                                    0,
                                    restartServerLog);
                    assertThat(recovered.key()).isEqualTo(initialKeys.get(0));
                    assertThat(recovered.value())
                            .isEqualTo(initialValues.get(0));
                    RecordMetadata appended =
                            produce(
                                    bootstrapServers,
                                    topic,
                                    secondKey,
                                    secondValue);
                    assertThat(appended.offset())
                            .isEqualTo(initialRecordCount);
                    ConsumerRecord<byte[], byte[]> fetched =
                            fetch(
                                    bootstrapServers,
                                    partition,
                                    initialRecordCount,
                                    restartServerLog);
                    assertThat(fetched.key()).isEqualTo(secondKey);
                    assertThat(fetched.value()).isEqualTo(secondValue);
                    try (Admin admin =
                            Admin.create(adminProperties(bootstrapServers))) {
                        assertOffsets(
                                admin,
                                partition,
                                0,
                                initialRecordCount + 1L);
                    }
                });
    }

    private static void awaitPositiveObjectCount(String bucket)
            throws InterruptedException {
        long deadline =
                System.nanoTime() + Duration.ofSeconds(45).toNanos();
        while (System.nanoTime() < deadline) {
            if (objectCount(bucket) > 0) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError(
                "Kafka NCP2 object did not appear before the process deadline");
    }

    private static void assertProcessLedgerDeletion(
            String bootstrapServers,
            TopicPartition partition,
            String topic,
            Path serverLog,
            String nereusCluster,
            BookKeeperWalConfiguration bookKeeperWal,
            BookKeeperProcessConfiguration bookKeeper,
            BookKeeper inspector
    ) throws Exception {
        KafkaPartitionId partitionId =
                kafkaPartitionId(
                        bootstrapServers,
                        partition,
                        topic);
        OxiaClientConfiguration oxia = oxiaConfiguration();
        Clock clock = Clock.systemUTC();
        BookKeeperMetadataStoreConfig metadataConfiguration =
                new BookKeeperMetadataStoreConfig(
                        bookKeeperWal.maxAppendRangesPerLedger(),
                        bookKeeperWal.protectionSlotsPerRange(),
                        bookKeeperWal.maxReaderLeasesPerLedger(),
                        bookKeeperWal.maxUncertainAllocations());
        try (SharedOxiaClientRuntime shared =
                        SharedOxiaClientRuntime.connect(
                                oxia,
                                clock);
                OxiaJavaKafkaPartitionMetadataStore partitions =
                        OxiaJavaKafkaPartitionMetadataStore
                                .usingSharedRuntime(
                                        oxia,
                                        shared,
                                        nereusCluster,
                                        partitionId.kafkaClusterId());
                OxiaJavaBookKeeperMetadataStore metadata =
                        OxiaJavaBookKeeperMetadataStore
                                .usingSharedRuntime(
                                        oxia,
                                        shared,
                                        clock,
                                        metadataConfiguration)) {
            StreamId streamId =
                    awaitPartitionStreamId(
                            partitions,
                            partitionId,
                            serverLog,
                            Duration.ofSeconds(30));
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord>
                    retired =
                            awaitProcessRetiredLedger(
                                    metadata,
                                    nereusCluster,
                                    streamId,
                                    serverLog,
                                    Duration.ofSeconds(45));
            long deletedLedgerId = retired.value().ledgerId();
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord>
                    deleted =
                            awaitProcessDeletedLedger(
                                    metadata,
                                    nereusCluster,
                                    bookKeeperWal.providerScopeSha256(),
                                    deletedLedgerId,
                                    serverLog,
                                    Duration.ofSeconds(45));
            assertThat(deleted.value().lifecycle())
                    .isEqualTo(BookKeeperLedgerLifecycle.DELETED);
            assertPhysicalLedgerAbsent(
                    inspector,
                    deletedLedgerId,
                    Files.readAllBytes(bookKeeper.passwordFile()));
        }
    }

    private static KafkaPartitionId kafkaPartitionId(
            String bootstrapServers,
            TopicPartition partition,
            String topic
    ) throws Exception {
        try (Admin admin =
                Admin.create(adminProperties(bootstrapServers))) {
            String kafkaClusterId =
                    admin.describeCluster()
                            .clusterId()
                            .get(
                                    CLIENT_TIMEOUT.toSeconds(),
                                    TimeUnit.SECONDS);
            String topicId =
                    admin.describeTopics(List.of(topic))
                            .allTopicNames()
                            .get(
                                    CLIENT_TIMEOUT.toSeconds(),
                                    TimeUnit.SECONDS)
                            .get(topic)
                            .topicId()
                            .toString();
            return new KafkaPartitionId(
                    kafkaClusterId,
                    topicId,
                    partition.partition());
        }
    }

    private static StreamId awaitPartitionStreamId(
            OxiaJavaKafkaPartitionMetadataStore partitions,
            KafkaPartitionId partitionId,
            Path serverLog,
            Duration timeout
    ) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            var binding = partitions.get(partitionId).join();
            if (binding.isPresent()
                    && !binding.orElseThrow().value().streamId().isBlank()) {
                return new StreamId(
                        binding.orElseThrow().value().streamId());
            }
            pauseForProviderState(
                    "Kafka process partition binding");
        }
        throw new AssertionError(
                "Kafka process partition binding did not publish a stream before the deadline:\n"
                        + readLog(serverLog));
    }

    private static BookKeeperInFlightEvidence
            awaitBookKeeperWritingReservation(
                    String nereusCluster,
                    KafkaPartitionId partitionId,
                    BookKeeperWalConfiguration bookKeeperWal,
                    long appliedEntryId,
                    Path serverLog,
                    Duration timeout) {
        OxiaClientConfiguration oxia = oxiaConfiguration();
        Clock clock = Clock.systemUTC();
        BookKeeperMetadataStoreConfig metadataConfiguration =
                new BookKeeperMetadataStoreConfig(
                        bookKeeperWal.maxAppendRangesPerLedger(),
                        bookKeeperWal.protectionSlotsPerRange(),
                        bookKeeperWal.maxReaderLeasesPerLedger(),
                        bookKeeperWal.maxUncertainAllocations());
        try (SharedOxiaClientRuntime shared =
                        SharedOxiaClientRuntime.connect(oxia, clock);
                OxiaJavaKafkaPartitionMetadataStore partitions =
                        OxiaJavaKafkaPartitionMetadataStore
                                .usingSharedRuntime(
                                        oxia,
                                        shared,
                                        nereusCluster,
                                        partitionId.kafkaClusterId());
                OxiaJavaBookKeeperMetadataStore metadata =
                        OxiaJavaBookKeeperMetadataStore
                                .usingSharedRuntime(
                                        oxia,
                                        shared,
                                        clock,
                                        metadataConfiguration)) {
            StreamId streamId =
                    awaitPartitionStreamId(
                            partitions,
                            partitionId,
                            serverLog,
                            timeout);
            long deadline = System.nanoTime() + timeout.toNanos();
            BookKeeperAppendReservationRecord last = null;
            while (System.nanoTime() < deadline) {
                var writer =
                        metadata.getWriter(nereusCluster, streamId)
                                .join();
                if (writer.isPresent()
                        && !writer.orElseThrow()
                                .value()
                                .activeReservationId()
                                .isEmpty()) {
                    String reservationId =
                            writer.orElseThrow()
                                    .value()
                                    .activeReservationId();
                    var reservation =
                            metadata.getReservation(
                                            nereusCluster,
                                            streamId,
                                            reservationId)
                                    .join();
                    if (reservation.isPresent()) {
                        last = reservation.orElseThrow().value();
                        if (last.lifecycle()
                                == AppendReservationLifecycle.WRITING) {
                            assertThat(last.streamId())
                                    .isEqualTo(streamId.value());
                            assertThat(last.firstEntryId())
                                    .isEqualTo(appliedEntryId);
                            assertThat(last.entryCount()).isEqualTo(1);
                            return new BookKeeperInFlightEvidence(
                                    streamId,
                                    reservationId,
                                    last.ledgerId(),
                                    appliedEntryId);
                        }
                    }
                }
                pauseForProviderState(
                        "BookKeeper WRITING reservation");
            }
            throw new AssertionError(
                    "provider-applied BookKeeper append did not remain WRITING before the deadline; last="
                            + last
                            + ":\n"
                            + readLog(serverLog));
        }
    }

    private static void awaitBookKeeperTakeoverReconciliation(
            String nereusCluster,
            BookKeeperWalConfiguration bookKeeperWal,
            BookKeeperInFlightEvidence inFlight,
            Path serverLog,
            Duration timeout
    ) {
        OxiaClientConfiguration oxia = oxiaConfiguration();
        Clock clock = Clock.systemUTC();
        BookKeeperMetadataStoreConfig metadataConfiguration =
                new BookKeeperMetadataStoreConfig(
                        bookKeeperWal.maxAppendRangesPerLedger(),
                        bookKeeperWal.protectionSlotsPerRange(),
                        bookKeeperWal.maxReaderLeasesPerLedger(),
                        bookKeeperWal.maxUncertainAllocations());
        try (SharedOxiaClientRuntime shared =
                        SharedOxiaClientRuntime.connect(oxia, clock);
                OxiaJavaBookKeeperMetadataStore metadata =
                        OxiaJavaBookKeeperMetadataStore
                                .usingSharedRuntime(
                                        oxia,
                                        shared,
                                        clock,
                                        metadataConfiguration)) {
            long deadline = System.nanoTime() + timeout.toNanos();
            BookKeeperAppendReservationRecord lastReservation = null;
            BookKeeperLedgerRootRecord lastRoot = null;
            while (System.nanoTime() < deadline) {
                var reservation =
                        metadata.getReservation(
                                        nereusCluster,
                                        inFlight.streamId(),
                                        inFlight.reservationId())
                                .join();
                var root =
                        metadata.getRoot(
                                        nereusCluster,
                                        bookKeeperWal
                                                .providerScopeSha256(),
                                        inFlight.ledgerId())
                                .join();
                if (reservation.isPresent()) {
                    lastReservation =
                            reservation.orElseThrow().value();
                }
                if (root.isPresent()) {
                    lastRoot = root.orElseThrow().value();
                }
                if (lastReservation != null
                        && lastRoot != null
                        && lastReservation.lifecycle()
                                == AppendReservationLifecycle.ABANDONED
                        && lastRoot.lifecycle()
                                == BookKeeperLedgerLifecycle.SEALED) {
                    assertThat(lastReservation.reservationId())
                            .isEqualTo(inFlight.reservationId());
                    assertThat(lastReservation.streamId())
                            .isEqualTo(inFlight.streamId().value());
                    assertThat(lastReservation.ledgerId())
                            .isEqualTo(inFlight.ledgerId());
                    assertThat(lastReservation.firstEntryId())
                            .isEqualTo(inFlight.entryId());
                    assertThat(lastReservation.stateReason())
                            .isNotBlank();
                    assertThat(lastRoot.streamId())
                            .isEqualTo(inFlight.streamId().value());
                    assertThat(lastRoot.providerScopeSha256())
                            .isEqualTo(
                                    bookKeeperWal
                                            .providerScopeSha256());
                    assertThat(lastRoot.ledgerId())
                            .isEqualTo(inFlight.ledgerId());
                    assertThat(lastRoot.sealedLastEntryId())
                            .isGreaterThanOrEqualTo(
                                    inFlight.entryId());
                    return;
                }
                pauseForProviderState(
                        "BookKeeper takeover reconciliation");
            }
            throw new AssertionError(
                    "BookKeeper takeover did not abandon the provider-applied WRITING reservation and seal its ledger; "
                            + "lastReservation="
                            + lastReservation
                            + ", lastRoot="
                            + lastRoot
                            + ":\n"
                            + readLog(serverLog));
        }
    }

    private static BookKeeperVersionedValue<BookKeeperLedgerRootRecord>
            awaitProcessRetiredLedger(
                    OxiaJavaBookKeeperMetadataStore metadata,
                    String nereusCluster,
                    StreamId streamId,
                    Path serverLog,
                    Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            ArrayList<
                            BookKeeperVersionedValue<
                                    BookKeeperLedgerRootRecord>>
                    candidates = new ArrayList<>();
            for (int shard = 0;
                    shard < BookKeeperKeyspace.LEDGER_SHARDS;
                    shard++) {
                Optional<BookKeeperScanToken> continuation =
                        Optional.empty();
                do {
                    var page =
                            metadata.scanRoots(
                                            nereusCluster,
                                            shard,
                                            continuation,
                                            100)
                                    .join();
                    page.values().stream()
                            .filter(
                                    value ->
                                            value.value()
                                                    .streamId()
                                                    .equals(
                                                            streamId
                                                                    .value()))
                            .filter(
                                    value ->
                                            switch (value
                                                    .value()
                                                    .lifecycle()) {
                                                case SEALED,
                                                        MARKED,
                                                        DELETING,
                                                        DELETED ->
                                                        true;
                                                default -> false;
                                            })
                            .forEach(candidates::add);
                    continuation = page.continuation();
                } while (continuation.isPresent());
            }
            Optional<
                            BookKeeperVersionedValue<
                                    BookKeeperLedgerRootRecord>>
                    earliest =
                            candidates.stream()
                                    .min(
                                            java.util.Comparator
                                                    .comparingLong(
                                                            value ->
                                                                    value.value()
                                                                            .segmentSequence()));
            if (earliest.isPresent()) {
                return earliest.orElseThrow();
            }
            pauseForProviderState(
                    "retired process BookKeeper ledger");
        }
        throw new AssertionError(
                "Kafka process did not roll a BookKeeper ledger before the deadline:\n"
                        + readLog(serverLog));
    }

    private static BookKeeperVersionedValue<BookKeeperLedgerRootRecord>
            awaitProcessDeletedLedger(
                    OxiaJavaBookKeeperMetadataStore metadata,
                    String nereusCluster,
                    String providerScopeSha256,
                    long ledgerId,
                    Path serverLog,
                    Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        BookKeeperVersionedValue<BookKeeperLedgerRootRecord> last =
                null;
        while (System.nanoTime() < deadline) {
            BookKeeperVersionedValue<BookKeeperLedgerRootRecord>
                    current =
                            metadata.getRoot(
                                            nereusCluster,
                                            providerScopeSha256,
                                            ledgerId)
                                    .join()
                                    .orElseThrow();
            last = current;
            if (current.value().lifecycle()
                    == BookKeeperLedgerLifecycle.DELETED) {
                return current;
            }
            if (current.value().lifecycle()
                            == BookKeeperLedgerLifecycle.QUARANTINED
                    || current.value().lifecycle()
                            == BookKeeperLedgerLifecycle.ABORTED) {
                throw new AssertionError(
                        "Kafka process BookKeeper retention entered terminal "
                                + current.value().lifecycle()
                                + ":\n"
                                + readLog(serverLog));
            }
            pauseForProviderState(
                    "deleted process BookKeeper ledger");
        }
        throw new AssertionError(
                "Kafka process BookKeeper ledger did not reach DELETED before the deadline; last="
                        + last
                        + ":\n"
                        + readLog(serverLog));
    }

    private static void assertPhysicalLedgerAbsent(
            BookKeeper inspector,
            long ledgerId,
            byte[] password
    ) throws Exception {
        try {
            var handle =
                    inspector.openLedgerNoRecovery(
                            ledgerId,
                            BookKeeper.DigestType.CRC32C,
                            password);
            try {
                handle.close();
            } finally {
                throw new AssertionError(
                        "physically deleted BookKeeper ledger reopened: "
                                + ledgerId);
            }
        } catch (BKException failure) {
            assertThat(failure.getCode())
                    .isIn(
                            BKException.Code
                                    .NoSuchLedgerExistsException,
                            BKException.Code
                                    .NoSuchLedgerExistsOnMetadataServerException);
        }
    }

    private static void assertPhysicalBookKeeperEntry(
            BookKeeper inspector,
            long ledgerId,
            long entryId,
            byte[] password
    ) throws Exception {
        LedgerHandle handle = null;
        try {
            handle =
                    inspector.openLedgerNoRecovery(
                            ledgerId,
                            BookKeeper.DigestType.CRC32C,
                            password);
            try (var entries =
                    handle.readUnconfirmed(entryId, entryId)) {
                var iterator = entries.iterator();
                assertThat(iterator.hasNext())
                        .as(
                                "provider-applied BookKeeper entry "
                                        + ledgerId
                                        + ":"
                                        + entryId)
                        .isTrue();
                var entry = iterator.next();
                assertThat(entry.getEntryId()).isEqualTo(entryId);
                assertThat(entry.getLength()).isPositive();
                assertThat(iterator.hasNext()).isFalse();
            }
        } finally {
            java.util.Arrays.fill(password, (byte) 0);
            if (handle != null) {
                handle.close();
            }
        }
    }

    private static void pauseForProviderState(String state) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted while awaiting " + state,
                    failure);
        }
    }

    private static void assertOffsets(
            Admin admin,
            TopicPartition partition,
            long earliestOffset,
            long latestOffset
    ) throws Exception {
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest =
                admin.listOffsets(Map.of(partition, OffsetSpec.earliest()))
                        .all()
                        .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest =
                admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                        .all()
                        .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        assertThat(earliest.get(partition).offset()).isEqualTo(earliestOffset);
        assertThat(latest.get(partition).offset()).isEqualTo(latestOffset);
    }

    private static OxiaClientConfiguration oxiaConfiguration() {
        return new OxiaClientConfiguration(
                OXIA.getServiceAddress(),
                "default",
                Duration.ofSeconds(10),
                Duration.ofSeconds(30),
                10_000,
                1_024);
    }

    private static BookKeeperWalConfiguration bookKeeperWalConfiguration(
            BookKeeperProcessConfiguration bookKeeper,
            boolean physicalLedgerDeletion
    ) {
        return new BookKeeperWalConfiguration(
                bookKeeper.clusterAlias(),
                bookKeeper.providerScopeSha256(),
                bookKeeper.ledgerIdPrefixBits(),
                bookKeeper.ledgerIdPrefixValue(),
                bookKeeper.ledgerIdReservationId(),
                2,
                2,
                2,
                BookKeeperDigestType.CRC32C,
                new BookKeeperSecretRef(
                        bookKeeper.passwordFile().toUri().toASCIIString(),
                        bookKeeper.passwordVersion()),
                physicalLedgerDeletion ? 1 : 100_000,
                256L * 1024 * 1024,
                1_000,
                8,
                64,
                32,
                Duration.ofHours(1),
                8,
                physicalLedgerDeletion ? 8 : 2,
                64L * 1024 * 1024,
                physicalLedgerDeletion
                        ? Duration.ofSeconds(4)
                        : Duration.ofSeconds(30),
                physicalLedgerDeletion
                        ? Duration.ofSeconds(4)
                        : Duration.ofSeconds(20),
                physicalLedgerDeletion
                        ? Duration.ofSeconds(4)
                        : Duration.ofSeconds(30),
                physicalLedgerDeletion
                        ? Duration.ofSeconds(4)
                        : Duration.ofSeconds(30),
                physicalLedgerDeletion
                        ? Duration.ofSeconds(5)
                        : Duration.ofMinutes(2),
                physicalLedgerDeletion
                        ? Duration.ofSeconds(1)
                        : Duration.ofSeconds(30),
                physicalLedgerDeletion
                        ? Duration.ofSeconds(1)
                        : Duration.ofMinutes(1),
                256);
    }

    private static BookKeeperProcessConfiguration
            bookKeeperProcessConfiguration(
                    String metadataServiceUri,
                    String fixtureToken,
                    Path passwordFile,
                    int authoritySeed,
                    int persistentBrokerCount
            ) {
        return new BookKeeperProcessConfiguration(
                metadataServiceUri,
                "kafka-" + fixtureToken + "-deployment",
                "primary-" + fixtureToken,
                String.format("%02x", 0x10 + authoritySeed).repeat(32),
                12,
                0x800L + authoritySeed,
                "kafka-" + fixtureToken + "-reservation",
                passwordFile,
                "v1",
                1,
                String.format("%02x", 0x54 + authoritySeed).repeat(32),
                persistentBrokerCount);
    }

    private static void seedBookKeeperAuthority(
            OxiaClientConfiguration oxia,
            BookKeeperWalConfiguration configuration,
            BookKeeperProcessConfiguration bookKeeper,
            Clock clock
    ) {
        try (SharedOxiaClientRuntime shared =
                SharedOxiaClientRuntime.connect(oxia, clock)) {
            OxiaBookKeeperLedgerIdNamespaceReservationStore namespaces =
                    new OxiaBookKeeperLedgerIdNamespaceReservationStore(
                            oxia,
                            shared);
            var reservation =
                    new BookKeeperLedgerIdNamespaceProvisioningCoordinator(
                                    namespaces,
                                    clock)
                            .provision(
                                    configuration,
                                    bookKeeper.deploymentId(),
                                    "44".repeat(32),
                                    Duration.ofSeconds(30))
                            .join();
            OxiaBookKeeperProtocolActivationStore activations =
                    new OxiaBookKeeperProtocolActivationStore(oxia, shared);
            BookKeeperProtocolActivationCoordinator coordinator =
                    new BookKeeperProtocolActivationCoordinator(
                            activations,
                            clock);
            var prepared = coordinator.prepare(
                            configuration,
                            reservation,
                            bookKeeper.readinessEpoch(),
                            bookKeeper.readinessSha256(),
                            Duration.ofSeconds(30))
                    .join();
            coordinator.activate(
                            configuration,
                            reservation,
                            BookKeeperProtocolActivationUpdate.publications(
                                    bookKeeper.readinessEpoch(),
                                    bookKeeper.readinessSha256(),
                                    true,
                                    true,
                                    prepared.metadataVersion()),
                            Duration.ofSeconds(30))
                    .join();
        }
    }

    private static LocalBookKeeper startBookKeeper(
            int zooKeeperPort
    ) throws Exception {
        ServerConfiguration configuration = new ServerConfiguration();
        configuration.setJournalSyncData(false);
        configuration.setJournalWriteData(false);
        configuration.setProperty("journalMaxGroupWaitMSec", 0L);
        configuration.setProperty("journalPreAllocSizeMB", 1);
        configuration.setFlushInterval(60_000);
        configuration.setGcWaitTime(60_000);
        configuration.setAllowLoopback(true);
        configuration.setAdvertisedAddress("127.0.0.1");
        configuration.setAllowEphemeralPorts(true);
        configuration.setNumAddWorkerThreads(0);
        configuration.setNumReadWorkerThreads(0);
        configuration.setNumHighPriorityWorkerThreads(0);
        configuration.setNumJournalCallbackThreads(0);
        configuration.setServerNumIOThreads(1);
        configuration.setNumLongPollWorkerThreads(1);
        configuration.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        configuration.setLedgerManagerFactoryClass(
                LongHierarchicalLedgerManagerFactory.class);
        configuration.setLedgerStorageClass(
                "org.apache.bookkeeper.bookie.SortedLedgerStorage");
        configuration.setDiskUsageThreshold(0.999F);
        configuration.setDiskUsageWarnThreshold(0.99F);
        LocalBookKeeper cluster =
                LocalBookKeeper.getLocalBookies(
                        "127.0.0.1",
                        zooKeeperPort,
                        2,
                        true,
                        configuration);
        cluster.start();
        return cluster;
    }

    private static BookKeeper bookKeeperClient(
            String metadataServiceUri
    ) throws Exception {
        ClientConfiguration configuration =
                new ClientConfiguration();
        configuration.setMetadataServiceUri(metadataServiceUri);
        return new BookKeeper(configuration);
    }

    private static RecordMetadata produce(
            String bootstrapServers,
            String topic,
            byte[] key,
            byte[] value
    ) throws InterruptedException, ExecutionException, TimeoutException {
        try (KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(producerProperties(bootstrapServers))) {
            return producer.send(new ProducerRecord<>(topic, 0, key, value))
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        }
    }

    private static PendingProduce beginSingleAttemptProduce(
            String bootstrapServers,
            String topic,
            byte[] key,
            byte[] value
    ) {
        Properties properties = producerProperties(bootstrapServers);
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "0");
        KafkaProducer<byte[], byte[]> producer =
                new KafkaProducer<>(properties);
        try {
            Future<RecordMetadata> future =
                    producer.send(
                            new ProducerRecord<>(
                                    topic,
                                    0,
                                    key,
                                    value));
            return new PendingProduce(producer, future);
        } catch (Throwable failure) {
            producer.close(Duration.ZERO);
            throw failure;
        }
    }

    private static RecordMetadata transactionalProduce(
            String bootstrapServers,
            String transactionalId,
            String topic,
            byte[] key,
            byte[] value
    ) throws Exception {
        Properties properties = producerProperties(bootstrapServers);
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "30000");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            producer.initTransactions();
            producer.beginTransaction();
            try {
                RecordMetadata metadata =
                        producer.send(new ProducerRecord<>(topic, 0, key, value))
                                .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                producer.commitTransaction();
                return metadata;
            } catch (Exception | AssertionError failure) {
                try {
                    producer.abortTransaction();
                } catch (RuntimeException abortFailure) {
                    failure.addSuppressed(abortFailure);
                }
                throw failure;
            }
        }
    }

    private static OpenTransaction beginTransaction(
            String bootstrapServers,
            String transactionalId,
            String topic,
            byte[] key,
            byte[] value
    ) throws Exception {
        Properties properties = producerProperties(bootstrapServers);
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "30000");
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);
        try {
            producer.initTransactions();
            producer.beginTransaction();
            RecordMetadata metadata =
                    producer.send(new ProducerRecord<>(topic, 0, key, value))
                            .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            producer.flush();
            return new OpenTransaction(producer, metadata);
        } catch (Exception | AssertionError failure) {
            producer.close(Duration.ZERO);
            throw failure;
        }
    }

    private static Properties producerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");
        return properties;
    }

    private static ConsumerRecord<byte[], byte[]> consumeGroupThroughOffset(
            String bootstrapServers,
            String groupId,
            String topic,
            TopicPartition partition,
            long expectedFirstOffset,
            long targetOffset,
            Path serverLog
    ) {
        Properties properties = consumerProperties(bootstrapServers, groupId);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + CLIENT_TIMEOUT.toNanos();
            Long firstOffset = null;
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {
                    if (firstOffset == null) {
                        firstOffset = record.offset();
                    }
                    if (record.offset() == targetOffset) {
                        assertThat(firstOffset).isEqualTo(expectedFirstOffset);
                        consumer.commitSync(
                                Map.of(
                                        partition,
                                        new OffsetAndMetadata(targetOffset + 1)),
                                CLIENT_TIMEOUT);
                        return record;
                    }
                }
            }
        }
        throw new AssertionError(
                "consumer group did not reach offset " + targetOffset + ":\n"
                        + readLog(serverLog));
    }

    private static long committedGroupOffset(
            String bootstrapServers,
            String groupId,
            TopicPartition partition
    ) {
        try (KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(consumerProperties(bootstrapServers, groupId))) {
            OffsetAndMetadata committed =
                    consumer.committed(Set.of(partition), CLIENT_TIMEOUT).get(partition);
            assertThat(committed)
                    .as("consumer group must have a committed offset for " + partition)
                    .isNotNull();
            return committed.offset();
        }
    }

    private static Properties consumerProperties(
            String bootstrapServers,
            String groupId
    ) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
        return properties;
    }

    private static ConsumerRecord<byte[], byte[]> fetch(
            String bootstrapServers,
            TopicPartition partition,
            long offset,
            Path serverLog
    ) throws Exception {
        return fetch(
                bootstrapServers,
                partition,
                offset,
                "read_uncommitted",
                serverLog);
    }

    private static ConsumerRecord<byte[], byte[]> fetchReadCommitted(
            String bootstrapServers,
            TopicPartition partition,
            long offset,
            Path serverLog
    ) throws Exception {
        return fetch(
                bootstrapServers,
                partition,
                offset,
                "read_committed",
                serverLog);
    }

    private static ConsumerRecord<byte[], byte[]> fetch(
            String bootstrapServers,
            TopicPartition partition,
            long offset,
            String isolationLevel,
            Path serverLog
    ) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "f9-process-" + UUID.randomUUID());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seek(partition, offset);
            long deadline = System.nanoTime() + CLIENT_TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records =
                        consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }
        throw new AssertionError("Fetch timed out:\n" + readLog(serverLog));
    }

    private static void runBroker(
            Path startScript,
            Path kafkaHome,
            Path config,
            Path formatLog,
            Path serverLog,
            String bootstrapServers,
            BrokerAssertions assertions
    ) throws Exception {
        runBroker(
                startScript,
                kafkaHome,
                config,
                formatLog,
                serverLog,
                bootstrapServers,
                StopMode.NORMAL,
                assertions);
    }

    private static void runBroker(
            Path startScript,
            Path kafkaHome,
            Path config,
            Path formatLog,
            Path serverLog,
            String bootstrapServers,
            StopMode stopMode,
            BrokerAssertions assertions
    ) throws Exception {
        Process broker = start(
                List.of(startScript.toString(), config.toString()),
                kafkaHome,
                serverLog);
        Throwable failure = null;
        try {
            awaitBroker(bootstrapServers, broker, serverLog);
            assertions.verify();
        } catch (Exception | AssertionError operationFailure) {
            failure = operationFailure;
        }
        try {
            stopMode.stop(broker, serverLog);
        } catch (Exception | AssertionError shutdownFailure) {
            if (failure == null) {
                failure = shutdownFailure;
            } else {
                failure.addSuppressed(shutdownFailure);
            }
        }
        if (failure != null) {
            try {
                preserveFailureEvidence(config, formatLog, serverLog);
            } catch (AssertionError evidenceFailure) {
                failure.addSuppressed(evidenceFailure);
            }
            rethrow(failure);
        }
    }

    private static void stopBroker(Process broker, Path serverLog) throws Exception {
        if (broker.isAlive()) {
            broker.destroy();
        }
        awaitNormalBrokerShutdown(
                broker,
                serverLog);
    }

    private static void awaitNormalBrokerShutdown(
            Process broker,
            Path serverLog
    ) throws Exception {
        int exit = await(broker, PROCESS_TIMEOUT, "Nereus Kafka shutdown", serverLog);
        String output = readLog(serverLog);
        if (exit != 0 && exit != 143) {
            throw new AssertionError(
                    "unexpected process exit " + exit + ":\n" + output);
        }
        assertThat(output)
                .as("Kafka process must reach its normal shutdown completion path")
                .contains("shut down completed");
    }

    private static void killBroker(Process broker, Path serverLog) throws Exception {
        if (broker.isAlive()) {
            broker.destroyForcibly();
        }
        int exit = await(broker, PROCESS_TIMEOUT, "forced Nereus Kafka stop", serverLog);
        String output = readLog(serverLog);
        assertThat(exit)
                .withFailMessage(() -> "forced process exit was unexpectedly clean:\n" + output)
                .isNotZero();
        assertThat(output)
                .as("forced process stop must not reach the normal shutdown completion path")
                .doesNotContain("shut down completed");
    }

    private static void signalProcess(
            Process process,
            String signal,
            Path serverLog
    ) throws Exception {
        if (!process.isAlive()) {
            throw new AssertionError(
                    "cannot send SIG"
                            + signal
                            + " to an exited Kafka process:\n"
                            + readLog(serverLog));
        }
        Process command =
                new ProcessBuilder(
                                "kill",
                                "-" + signal,
                                Long.toString(process.pid()))
                        .redirectErrorStream(true)
                        .start();
        if (!command.waitFor(5, TimeUnit.SECONDS)) {
            command.destroyForcibly();
            throw new AssertionError(
                    "timed out sending SIG" + signal + " to Kafka process " + process.pid());
        }
        if (command.exitValue() != 0) {
            throw new AssertionError(
                    "failed to send SIG"
                            + signal
                            + " to Kafka process "
                            + process.pid());
        }
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception exact) {
            throw exact;
        }
        if (failure instanceof AssertionError exact) {
            throw exact;
        }
        throw new AssertionError("unexpected Kafka process failure", failure);
    }

    private static Throwable mergeFailure(
            Throwable current,
            Throwable additional
    ) {
        if (current == null) {
            return additional;
        }
        current.addSuppressed(additional);
        return current;
    }

    private static void awaitBroker(
            String bootstrapServers,
            Process broker,
            Path serverLog
    ) throws Exception {
        long deadline = System.nanoTime() + PROCESS_TIMEOUT.toNanos();
        Throwable lastFailure = null;
        while (System.nanoTime() < deadline) {
            if (!broker.isAlive()) {
                throw new AssertionError(
                        "Nereus Kafka exited before readiness:\n" + readLog(serverLog));
            }
            try (Admin admin = Admin.create(adminProperties(bootstrapServers))) {
                admin.describeCluster()
                        .nodes()
                        .get(2, TimeUnit.SECONDS);
                return;
            } catch (Throwable failure) {
                lastFailure = failure;
                Thread.sleep(250);
            }
        }
        throw new AssertionError(
                "Nereus Kafka did not become ready:\n" + readLog(serverLog),
                lastFailure);
    }

    private static String awaitMarker(
            Path marker,
            Process broker,
            Path serverLog,
            Duration timeout
    ) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(marker)) {
                return Files.readString(marker, StandardCharsets.UTF_8);
            }
            if (!broker.isAlive()) {
                throw new AssertionError(
                        "Kafka process exited before publishing marker "
                                + marker.getFileName()
                                + ":\n"
                                + readLog(serverLog));
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "Kafka process did not publish marker "
                        + marker.getFileName()
                        + " before the deadline:\n"
                        + readLog(serverLog));
    }

    private static String awaitAppliedMarker(
            Path marker,
            Process broker,
            PendingProduce pending,
            Path serverLog,
            Duration timeout
    ) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(marker)) {
                return Files.readString(marker, StandardCharsets.UTF_8);
            }
            if (pending.future().isDone()) {
                Throwable failure = pending.awaitFailure();
                throw new AssertionError(
                        "BookKeeper write completed before the provider-applied marker:\n"
                                + readLog(serverLog),
                        failure);
            }
            if (!broker.isAlive()) {
                throw new AssertionError(
                        "Kafka process exited before publishing marker "
                                + marker.getFileName()
                                + ":\n"
                                + readLog(serverLog));
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "Kafka process did not publish marker "
                        + marker.getFileName()
                        + " before the deadline:\n"
                + readLog(serverLog));
    }

    private static void awaitActivationCutMarker(
            ActivationPublicationCut cut,
            ActivationAgentMarkers[] markers,
            int gatedControllerIndex,
            Process[] nodes,
            Path[] serverLogs,
            Duration timeout
    ) throws Exception {
        Path expectedMarker =
                cut.phase()
                        == ActivationFaultPhase
                                .BEFORE_PROVIDER
                        ? markers[
                                        gatedControllerIndex]
                                .blocked()
                        : markers[
                                        gatedControllerIndex]
                                .applied();
        String operation =
                awaitMarker(
                                expectedMarker,
                                nodes[gatedControllerIndex],
                                serverLogs[
                                        gatedControllerIndex],
                                timeout)
                        .strip();
        assertThat(operation)
                .isEqualTo(
                        cut.operation());
        Path unexpectedMarker =
                cut.phase()
                        == ActivationFaultPhase
                                .BEFORE_PROVIDER
                        ? markers[
                                        gatedControllerIndex]
                                .applied()
                        : markers[
                                        gatedControllerIndex]
                                .blocked();
        assertThat(
                        Files.exists(
                                unexpectedMarker))
                .as(
                        "the gated controller must expose only the selected activation fault phase")
                .isFalse();
        for (int index = 0;
                index < markers.length;
                index++) {
            if (index
                    == gatedControllerIndex) {
                continue;
            }
            assertThat(
                            Files.exists(
                                    markers[index]
                                            .captured()))
                    .as(
                            "only the armed current controller may capture the activation cut")
                    .isFalse();
            assertThat(
                            Files.exists(
                                    markers[index]
                                            .blocked()))
                    .isFalse();
            assertThat(
                            Files.exists(
                                    markers[index]
                                            .applied()))
                    .isFalse();
        }
    }

    private static void awaitClusterBrokers(
            String bootstrapServers,
            List<Integer> expectedBrokerIds,
            List<Process> brokers,
            Path... serverLogs
    ) throws Exception {
        List<Integer> expected = expectedBrokerIds.stream().sorted().toList();
        long deadline = System.nanoTime() + PROCESS_TIMEOUT.toNanos();
        Throwable lastFailure = null;
        while (System.nanoTime() < deadline) {
            assertProcessesAlive(brokers, serverLogs);
            try (Admin admin = Admin.create(adminProperties(bootstrapServers))) {
                List<Integer> observed = admin.describeCluster()
                        .nodes()
                        .get(2, TimeUnit.SECONDS)
                        .stream()
                        .map(node -> node.id())
                        .sorted()
                        .toList();
                if (observed.equals(expected)) {
                    return;
                }
                lastFailure = new AssertionError(
                        "expected Kafka brokers " + expected
                                + " but observed " + observed);
            } catch (Throwable failure) {
                lastFailure = failure;
            }
            Thread.sleep(250);
        }
        throw new AssertionError(
                "Kafka cluster did not expose brokers " + expected
                        + " before the deadline:\n"
                        + joinedLogs(serverLogs),
                lastFailure);
    }

    private static ControllerQuorumEvidence
            awaitControllerQuorum(
                    String bootstrapServers,
                    List<Integer> expectedVoterIds,
                    int disallowedLeaderId,
                    long minimumExclusiveEpoch,
                    List<Process> liveProcesses,
                    Path... serverLogs
            ) throws Exception {
        return awaitControllerQuorum(
                adminProperties(
                        bootstrapServers),
                expectedVoterIds,
                disallowedLeaderId,
                minimumExclusiveEpoch,
                liveProcesses,
                serverLogs);
    }

    private static ControllerQuorumEvidence
            awaitControllerQuorum(
                    Properties adminConfiguration,
                    List<Integer> expectedVoterIds,
                    int disallowedLeaderId,
                    long minimumExclusiveEpoch,
                    List<Process> liveProcesses,
                    Path... serverLogs
            ) throws Exception {
        List<Integer> expected =
                expectedVoterIds.stream().sorted().toList();
        long deadline =
                System.nanoTime()
                        + PROCESS_TIMEOUT.toNanos();
        Throwable lastFailure = null;
        while (System.nanoTime() < deadline) {
            assertProcessesAlive(
                    liveProcesses,
                    serverLogs);
            try (Admin admin =
                    Admin.create(
                            adminConfiguration)) {
                QuorumInfo quorum =
                        admin.describeMetadataQuorum()
                                .quorumInfo()
                                .get(
                                        2,
                                        TimeUnit.SECONDS);
                List<Integer> voters =
                        quorum.voters()
                                .stream()
                                .map(
                                        QuorumInfo.ReplicaState
                                                ::replicaId)
                                .sorted()
                                .toList();
                if (voters.equals(expected)
                        && quorum.leaderId() >= 0
                        && quorum.leaderId()
                                != disallowedLeaderId
                        && quorum.leaderEpoch()
                                > minimumExclusiveEpoch
                        && quorum.highWatermark() >= 0) {
                    return new ControllerQuorumEvidence(
                            quorum.leaderId(),
                            quorum.leaderEpoch(),
                            quorum.highWatermark());
                }
                lastFailure =
                        new AssertionError(
                                "expected controller voters "
                                        + expected
                                        + ", leader other than "
                                        + disallowedLeaderId
                                        + " and epoch > "
                                        + minimumExclusiveEpoch
                                        + " but observed "
                                        + quorum);
            } catch (Throwable failure) {
                lastFailure = failure;
            }
            Thread.sleep(250);
        }
        throw new AssertionError(
                "KRaft controller quorum did not reach the expected state:\n"
                        + joinedLogs(serverLogs),
                lastFailure);
    }

    private static void
            awaitControllerActivationReconciliation(
                    Process controller,
                    Path serverLog,
                    ControllerQuorumEvidence quorum
            ) throws Exception {
        String expected =
                activationReconciliationMarker(
                        quorum);
        long deadline =
                System.nanoTime()
                        + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (!controller.isAlive()) {
                throw new AssertionError(
                        "controller exited before Nereus activation reconciliation:\n"
                                + readLog(serverLog));
            }
            if (readLog(serverLog).contains(expected)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "controller did not reconcile Nereus activation for the elected epoch; expected '"
                        + expected
                        + "':\n"
                        + readLog(serverLog));
    }

    private static String activationReconciliationMarker(
            ControllerQuorumEvidence quorum
    ) {
        return "Nereus Kafka storage activation reconciled by controller "
                + quorum.leaderId()
                + " at epoch "
                + quorum.leaderEpoch();
    }

    private static KafkaActivationCutEvidence
            awaitActivationCutEvidence(
                    String nereusCluster,
                    String kafkaClusterId,
                    List<Integer> expectedBrokerIds,
                    ActivationPublicationCut cut,
                    Path[] serverLogs,
                    Duration timeout
            ) {
        if (cut.durableState()
                == ActivationDurableState.ABSENT) {
            return new KafkaActivationCutEvidence(
                    Optional.empty(),
                    awaitAbsentActivationReadiness(
                            nereusCluster,
                            kafkaClusterId,
                            expectedBrokerIds,
                            serverLogs,
                            timeout));
        }
        KafkaActivationEvidence evidence =
                awaitActivationLifecycle(
                        nereusCluster,
                        kafkaClusterId,
                        expectedBrokerIds,
                        cut.durableState()
                                .lifecycle(),
                        serverLogs,
                        timeout);
        return new KafkaActivationCutEvidence(
                Optional.of(
                        evidence.activation()),
                evidence.readiness());
    }

    private static KafkaStorageReadinessRecord
            awaitAbsentActivationReadiness(
                    String nereusCluster,
                    String kafkaClusterId,
                    List<Integer> expectedBrokerIds,
                    Path[] serverLogs,
                    Duration timeout
            ) {
        List<Integer> expected =
                expectedBrokerIds.stream()
                        .sorted()
                        .toList();
        OxiaClientConfiguration oxia =
                oxiaConfiguration();
        Clock clock = Clock.systemUTC();
        long deadline =
                System.nanoTime()
                        + timeout.toNanos();
        Throwable lastFailure = null;
        try (SharedOxiaClientRuntime shared =
                        SharedOxiaClientRuntime.connect(
                                oxia,
                                clock);
                KafkaStorageActivationMetadataStore store =
                        KafkaStorageActivationMetadataStore
                                .usingSharedRuntime(
                                        oxia,
                                        shared,
                                        nereusCluster,
                                        kafkaClusterId)) {
            while (System.nanoTime() < deadline) {
                try {
                    var activation =
                            store.getActivation()
                                    .join();
                    var readiness =
                            store.getReadiness()
                                    .join();
                    if (activation.isEmpty()
                            && readiness.isPresent()) {
                        KafkaStorageReadinessRecord ready =
                                readiness.orElseThrow()
                                        .value();
                        List<Integer> brokers =
                                ready.brokers()
                                        .stream()
                                        .map(
                                                KafkaBrokerIdentity
                                                        ::brokerId)
                                        .sorted()
                                        .toList();
                        if (ready.kafkaClusterId()
                                        .equals(
                                                kafkaClusterId)
                                && brokers.equals(
                                        expected)
                                && ready.expiresAtMillis()
                                        > clock.millis()) {
                            return ready;
                        }
                        lastFailure =
                                new AssertionError(
                                        "activation is absent but readiness has not converged: "
                                                + ready);
                    } else {
                        lastFailure =
                                new AssertionError(
                                        "expected absent activation and present readiness but observed activation="
                                                + activation
                                                + ", readiness="
                                                + readiness);
                    }
                } catch (Throwable failure) {
                    lastFailure = failure;
                }
                pauseForProviderState(
                        "absent activation");
            }
        }
        throw new AssertionError(
                "Nereus absent-activation/readiness did not converge for brokers "
                        + expected
                        + ":\n"
                        + joinedLogs(
                                serverLogs),
                lastFailure);
    }

    private static KafkaActivationEvidence
            awaitActiveActivation(
                    String nereusCluster,
                    String kafkaClusterId,
                    List<Integer> expectedBrokerIds,
                    Path[] serverLogs,
                    Duration timeout
            ) {
        return awaitActivationLifecycle(
                nereusCluster,
                kafkaClusterId,
                expectedBrokerIds,
                KafkaStorageActivationLifecycle.ACTIVE,
                serverLogs,
                timeout);
    }

    private static KafkaActivationEvidence
            awaitActivationLifecycle(
                    String nereusCluster,
                    String kafkaClusterId,
                    List<Integer> expectedBrokerIds,
                    KafkaStorageActivationLifecycle expectedLifecycle,
                    Path[] serverLogs,
                    Duration timeout
            ) {
        List<Integer> expected =
                expectedBrokerIds.stream().sorted().toList();
        OxiaClientConfiguration oxia =
                oxiaConfiguration();
        Clock clock = Clock.systemUTC();
        long deadline =
                System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;
        try (SharedOxiaClientRuntime shared =
                        SharedOxiaClientRuntime.connect(
                                oxia,
                                clock);
                KafkaStorageActivationMetadataStore store =
                        KafkaStorageActivationMetadataStore
                                .usingSharedRuntime(
                                        oxia,
                                        shared,
                                        nereusCluster,
                                        kafkaClusterId)) {
            while (System.nanoTime() < deadline) {
                try {
                    var activation =
                            store.getActivation().join();
                    var readiness =
                            store.getReadiness().join();
                    if (activation.isPresent()
                            && readiness.isPresent()) {
                        KafkaStorageProtocolActivationRecord activationValue =
                                activation.orElseThrow().value();
                        KafkaStorageReadinessRecord ready =
                                readiness.orElseThrow().value();
                        List<Integer> brokers =
                                ready.brokers()
                                        .stream()
                                        .map(
                                                KafkaBrokerIdentity
                                                        ::brokerId)
                                        .sorted()
                                        .toList();
                        if (activationValue.lifecycle()
                                        == expectedLifecycle
                                && activationValue.kafkaClusterId()
                                        .equals(
                                                kafkaClusterId)
                                && ready.kafkaClusterId()
                                        .equals(
                                                kafkaClusterId)
                                && brokers.equals(expected)
                                && ready.readinessEpoch()
                                        >= activationValue
                                                .activationEpoch()
                                && ready.expiresAtMillis()
                                        > clock.millis()
                                && java.util.Arrays.equals(
                                        activationValue
                                                .requiredCapabilitySha256(),
                                        ready
                                                .capabilitySha256())) {
                            return new KafkaActivationEvidence(
                                    activationValue,
                                    ready);
                        }
                        lastFailure =
                                new AssertionError(
                                        "activation/readiness has not converged: activation="
                                                + activationValue
                                                + ", readiness="
                                                + ready);
                    }
                } catch (Throwable failure) {
                    lastFailure = failure;
                }
                pauseForProviderState(
                        "multi-controller activation");
            }
        }
        throw new AssertionError(
                "Nereus "
                        + expectedLifecycle
                        + "/readiness did not converge for brokers "
                        + expected
                        + ":\n"
                        + joinedLogs(serverLogs),
                lastFailure);
    }

    private static void assertPreparedFactsPreserved(
            KafkaStorageProtocolActivationRecord prepared,
            KafkaStorageProtocolActivationRecord active
    ) {
        assertThat(prepared.lifecycle())
                .isEqualTo(
                        KafkaStorageActivationLifecycle
                                .PREPARED);
        assertThat(active.lifecycle())
                .isEqualTo(
                        KafkaStorageActivationLifecycle
                                .ACTIVE);
        assertThat(active.kafkaClusterId())
                .isEqualTo(
                        prepared.kafkaClusterId());
        assertThat(active.activationEpoch())
                .isEqualTo(
                        prepared.activationEpoch());
        assertThat(active.preparedAtMetadataOffset())
                .isEqualTo(
                        prepared.preparedAtMetadataOffset());
        assertThat(active.preparedAtMillis())
                .isEqualTo(
                        prepared.preparedAtMillis());
        assertThat(active.requiredCapabilitySha256())
                .containsExactly(
                        prepared.requiredCapabilitySha256());
        assertThat(active.requiredBrokerSetSha256())
                .containsExactly(
                        prepared.requiredBrokerSetSha256());
        assertThat(active.allowedStorageProfiles())
                .isEqualTo(
                        prepared.allowedStorageProfiles());
        assertThat(active.defaultStorageProfile())
                .isEqualTo(
                        prepared.defaultStorageProfile());
    }

    private static void awaitPartitionLeader(
            Admin admin,
            TopicPartition partition,
            int expectedLeaderId,
            Process brokerOne,
            Process brokerTwo,
            Path brokerOneLog,
            Path brokerTwoLog
    ) throws Exception {
        long deadline = System.nanoTime() + CLIENT_TIMEOUT.toNanos();
        Throwable lastFailure = null;
        while (System.nanoTime() < deadline) {
            assertProcessesAlive(
                    List.of(brokerOne, brokerTwo),
                    brokerOneLog,
                    brokerTwoLog);
            try {
                var description = admin.describeTopics(
                                List.of(partition.topic()))
                        .allTopicNames()
                        .get(2, TimeUnit.SECONDS)
                        .get(partition.topic());
                var partitionInfo = description.partitions()
                        .stream()
                        .filter(info ->
                                info.partition() == partition.partition())
                        .findFirst()
                        .orElseThrow();
                List<Integer> replicas = partitionInfo.replicas()
                        .stream()
                        .map(node -> node.id())
                        .toList();
                List<Integer> isr = partitionInfo.isr()
                        .stream()
                        .map(node -> node.id())
                        .toList();
                if (partitionInfo.leader() != null
                        && partitionInfo.leader().id() == expectedLeaderId
                        && replicas.equals(List.of(expectedLeaderId))
                        && isr.equals(List.of(expectedLeaderId))) {
                    return;
                }
                lastFailure = new AssertionError(
                        "expected leader/replicas/ISR ["
                                + expectedLeaderId
                                + "] but observed leader "
                                + (partitionInfo.leader() == null
                                        ? "<none>"
                                        : partitionInfo.leader().id())
                                + ", replicas " + replicas
                                + ", ISR " + isr);
            } catch (Throwable failure) {
                lastFailure = failure;
            }
            Thread.sleep(250);
        }
        throw new AssertionError(
                "partition did not complete the Nereus shared-storage handoff:\n"
                        + joinedLogs(brokerOneLog, brokerTwoLog),
                lastFailure);
    }

    private static void assertProcessesAlive(
            List<Process> processes,
            Path... serverLogs
    ) {
        for (int index = 0; index < processes.size(); index++) {
            if (!processes.get(index).isAlive()) {
                throw new AssertionError(
                        "Kafka process " + index
                                + " exited during live takeover:\n"
                                + joinedLogs(serverLogs));
            }
        }
    }

    private static String joinedLogs(Path... serverLogs) {
        StringBuilder joined = new StringBuilder();
        for (Path serverLog : serverLogs) {
            joined.append("\n===== ")
                    .append(serverLog.getFileName())
                    .append(" =====\n")
                    .append(readLog(serverLog));
        }
        return joined.toString();
    }

    private static Properties adminProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        properties.setProperty(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                "5000");
        properties.setProperty(
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                "5000");
        return properties;
    }

    private static Properties controllerAdminProperties(
            String bootstrapControllers
    ) {
        Properties properties = new Properties();
        properties.setProperty(
                AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG,
                bootstrapControllers);
        properties.setProperty(
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
                "5000");
        properties.setProperty(
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                "5000");
        return properties;
    }

    private static Process start(
            List<String> command,
            Path workingDirectory,
            Path output
    ) throws IOException {
        return start(command, workingDirectory, output, Map.of());
    }

    private static Process start(
            List<String> command,
            Path workingDirectory,
            Path output,
            Map<String, String> environmentOverrides
    ) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.directory(workingDirectory.toFile());
        builder.redirectErrorStream(true);
        builder.redirectOutput(output.toFile());
        Map<String, String> environment = builder.environment();
        environment.put("AWS_ACCESS_KEY_ID", LOCALSTACK.getAccessKey());
        environment.put("AWS_SECRET_ACCESS_KEY", LOCALSTACK.getSecretKey());
        environment.put("AWS_REGION", LOCALSTACK.getRegion());
        environment.put("AWS_DEFAULT_REGION", LOCALSTACK.getRegion());
        environment.put("AWS_EC2_METADATA_DISABLED", "true");
        environment.put("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m");
        environment.put("EXTRA_ARGS", "");
        environment.putAll(environmentOverrides);
        return builder.start();
    }

    private static int await(
            Process process,
            Duration timeout,
            String operation,
            Path log
    ) throws Exception {
        if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
            throw new AssertionError(operation + " timed out:\n" + readLog(log));
        }
        return process.exitValue();
    }

    private static Path requiredKafkaCheckout() {
        String configured = System.getProperty("nereus.kafka.fork.checkout");
        assertThat(configured)
                .as("nereus.kafka.fork.checkout")
                .isNotBlank();
        Path checkout = Path.of(configured).toAbsolutePath().normalize();
        assertThat(checkout.resolve(".git"))
                .as("configured Kafka fork checkout")
                .exists();
        return checkout;
    }

    private static Path requiredBookKeeperFaultAgent() {
        String configured =
                System.getProperty(
                        "nereus.kafka.bookkeeper.fault.agent");
        assertThat(configured)
                .as("nereus.kafka.bookkeeper.fault.agent")
                .isNotBlank();
        Path agent = Path.of(configured).toAbsolutePath().normalize();
        assertThat(agent)
                .as("configured BookKeeper fault-agent JAR")
                .exists()
                .isRegularFile();
        return agent;
    }

    private static Path requiredActivationFaultAgent() {
        String configured =
                System.getProperty(
                        "nereus.kafka.activation.fault.agent");
        assertThat(configured)
                .as(
                        "nereus.kafka.activation.fault.agent")
                .isNotBlank();
        Path agent =
                Path.of(configured)
                        .toAbsolutePath()
                        .normalize();
        assertThat(agent)
                .as(
                        "configured activation fault-agent JAR")
                .exists()
                .isRegularFile();
        return agent;
    }

    private static String activationFaultAgentOptions(
            Path agent,
            ActivationPublicationCut cut,
            ActivationAgentMarkers markers
    ) {
        Map<String, String> arguments =
                Map.of(
                        "operation",
                        cut.operation(),
                        "phase",
                        cut.phase().agentValue(),
                        "arm",
                        markers.arm().toString(),
                        "captured",
                        markers.captured().toString(),
                        "blocked",
                        markers.blocked().toString(),
                        "applied",
                        markers.applied().toString(),
                        "installed",
                        markers.installed().toString());
        arguments.forEach(
                (name, value) ->
                        assertThat(value)
                                .as(
                                        "activation fault-agent "
                                                + name
                                                + " argument")
                                .doesNotContain(
                                        ",",
                                        "="));
        assertThat(agent.toString())
                .as(
                        "activation fault-agent JAR path")
                .doesNotContain(" ");
        return "-javaagent:"
                + agent
                + "="
                + arguments.entrySet()
                        .stream()
                        .sorted(
                                Map.Entry
                                        .comparingByKey())
                        .map(
                                entry ->
                                        entry.getKey()
                                                + "="
                                                + entry.getValue())
                        .collect(
                                java.util.stream.Collectors
                                        .joining(","));
    }

    private static String bookKeeperFaultAgentOptions(
            Path agent,
            Path arm,
            Path captured,
            Path applied,
            Path release,
            Path installed
    ) {
        Map<String, Path> markers =
                Map.of(
                        "arm", arm,
                        "captured", captured,
                        "applied", applied,
                        "release", release,
                        "installed", installed);
        markers.forEach(
                (name, path) ->
                        assertThat(path.toString())
                                .as(
                                        "BookKeeper fault-agent "
                                                + name
                                                + " marker path")
                                .doesNotContain(",", "="));
        assertThat(agent.toString())
                .as("BookKeeper fault-agent JAR path")
                .doesNotContain(" ");
        return "-javaagent:"
                + agent
                + "=arm="
                + arm
                + ",captured="
                + captured
                + ",applied="
                + applied
                + ",release="
                + release
                + ",installed="
                + installed;
    }

    private static Path extractReleaseDistribution(
            Path checkout,
            Path target
    ) throws Exception {
        Path distributionDirectory = checkout.resolve("core/build/distributions");
        Path archive;
        try (var archives = Files.list(distributionDirectory)) {
            archive = archives
                    .filter(path -> path.getFileName().toString().startsWith("kafka_"))
                    .filter(path -> path.getFileName().toString().endsWith(".tgz"))
                    .filter(path -> !path.getFileName().toString().contains("site-docs"))
                    .max(java.util.Comparator.comparingLong(
                            NereusKafkaNativeProcessIntegrationTest::lastModified))
                    .orElseThrow(() -> new AssertionError(
                            "missing Kafka release distribution under " + distributionDirectory));
        }
        Files.createDirectories(target);
        Process extraction = new ProcessBuilder(
                        "tar",
                        "-xzf",
                        archive.toString(),
                        "-C",
                        target.toString())
                .redirectErrorStream(true)
                .redirectOutput(target.resolve("extract.log").toFile())
                .start();
        int exit = await(
                extraction,
                Duration.ofSeconds(30),
                "Kafka release extraction",
                target.resolve("extract.log"));
        assertThat(exit)
                .withFailMessage(() -> "Kafka release extraction failed:\n"
                        + readLog(target.resolve("extract.log")))
                .isZero();
        try (var children = Files.list(target)) {
            return children
                    .filter(Files::isDirectory)
                    .findFirst()
                    .orElseThrow(() -> new AssertionError(
                            "Kafka release archive has no top-level directory: " + archive));
        }
    }

    private static long lastModified(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException failure) {
            throw new IllegalStateException(
                    "cannot inspect distribution archive " + path,
                    failure);
        }
    }

    private static Path executable(Path path) {
        assertThat(path).exists().isExecutable();
        return path;
    }

    private static void preserveFailureEvidence(
            Path config,
            Path formatLog,
            Path... serverLogs
    ) {
        String configured = System.getProperty("nereus.kafka.process.evidence.dir");
        if (configured == null || configured.isBlank()) {
            return;
        }
        Path target = Path.of(configured).toAbsolutePath().normalize();
        try {
            Files.createDirectories(target);
            copyIfPresent(config, target.resolve("server.properties"));
            copyIfPresent(formatLog, target.resolve("format.log"));
            for (Path serverLog : serverLogs) {
                copyIfPresent(serverLog, target.resolve(serverLog.getFileName()));
            }
        } catch (IOException failure) {
            throw new AssertionError(
                    "failed to preserve Kafka process evidence under " + target,
                    failure);
        }
    }

    private static void preserveMultiBrokerFailureEvidence(
            Path brokerOneConfig,
            Path brokerTwoConfig,
            Path brokerOneFormatLog,
            Path brokerTwoFormatLog,
            Path brokerOneServerLog,
            Path brokerTwoServerLog
    ) {
        String configured = System.getProperty("nereus.kafka.process.evidence.dir");
        if (configured == null || configured.isBlank()) {
            return;
        }
        Path target = Path.of(configured).toAbsolutePath().normalize();
        try {
            Files.createDirectories(target);
            copyIfPresent(
                    brokerOneConfig,
                    target.resolve("multi-broker-one.properties"));
            copyIfPresent(
                    brokerTwoConfig,
                    target.resolve("multi-broker-two.properties"));
            copyIfPresent(
                    brokerOneFormatLog,
                    target.resolve("multi-broker-one-format.log"));
            copyIfPresent(
                    brokerTwoFormatLog,
                    target.resolve("multi-broker-two-format.log"));
            copyIfPresent(
                    brokerOneServerLog,
                    target.resolve("multi-broker-one-server.log"));
            copyIfPresent(
                    brokerTwoServerLog,
                    target.resolve("multi-broker-two-server.log"));
        } catch (IOException failure) {
            throw new AssertionError(
                    "failed to preserve multi-broker Kafka process evidence under "
                            + target,
                    failure);
        }
    }

    private static void
            preserveMultiControllerFailureEvidence(
                    Path[] configs,
                    Path[] formatLogs,
                    Path[] serverLogs
            ) {
        String configured =
                System.getProperty(
                        "nereus.kafka.process.evidence.dir");
        if (configured == null || configured.isBlank()) {
            return;
        }
        Path target =
                Path.of(configured)
                        .toAbsolutePath()
                        .normalize();
        try {
            Files.createDirectories(target);
            for (int index = 0;
                    index < configs.length;
                    index++) {
                int nodeId = index + 1;
                copyIfPresent(
                        configs[index],
                        target.resolve(
                                "multi-controller-node-"
                                        + nodeId
                                        + ".properties"));
                copyIfPresent(
                        formatLogs[index],
                        target.resolve(
                                "multi-controller-node-"
                                        + nodeId
                                        + "-format.log"));
                copyIfPresent(
                        serverLogs[index],
                        target.resolve(
                                "multi-controller-node-"
                                        + nodeId
                                        + "-server.log"));
            }
        } catch (IOException failure) {
            throw new AssertionError(
                    "failed to preserve multi-controller Kafka process evidence under "
                            + target,
                    failure);
        }
    }

    private static void
            preserveActivationCutFailureEvidence(
                    ActivationPublicationCut cut,
                    Path[] configs,
                    Path[] formatLogs,
                    Path[] serverLogs,
                    ActivationAgentMarkers[] markers
            ) {
        String configured =
                System.getProperty(
                        "nereus.kafka.process.evidence.dir");
        if (configured == null || configured.isBlank()) {
            return;
        }
        Path target =
                Path.of(configured)
                        .toAbsolutePath()
                        .normalize();
        try {
            Files.createDirectories(target);
            for (int index = 0;
                    index < configs.length;
                    index++) {
                int nodeId = index + 1;
                String prefix =
                        "activation-cut-"
                                + cut.slug()
                                + "-node-"
                                + nodeId;
                copyIfPresent(
                        configs[index],
                        target.resolve(
                                prefix
                                        + ".properties"));
                copyIfPresent(
                        formatLogs[index],
                        target.resolve(
                                prefix
                                        + "-format.log"));
                copyIfPresent(
                        serverLogs[index],
                        target.resolve(
                                prefix
                                        + "-server.log"));
            }
            for (int index = 0;
                    index < markers.length;
                    index++) {
                int nodeId = index + 1;
                String prefix =
                        "activation-cut-"
                                + cut.slug()
                                + "-controller-"
                                + nodeId
                                + "-agent-";
                copyIfPresent(
                        markers[index].arm(),
                        target.resolve(
                                prefix + "arm"));
                copyIfPresent(
                        markers[index].captured(),
                        target.resolve(
                                prefix + "captured"));
                copyIfPresent(
                        markers[index].blocked(),
                        target.resolve(
                                prefix + "blocked"));
                copyIfPresent(
                        markers[index].applied(),
                        target.resolve(
                                prefix + "applied"));
                copyIfPresent(
                        markers[index].installed(),
                        target.resolve(
                                prefix + "installed"));
            }
        } catch (IOException failure) {
            throw new AssertionError(
                    "failed to preserve activation-cut Kafka process evidence under "
                            + target,
                    failure);
        }
    }

    private static void preserveInFlightTakeoverFailureEvidence(
            Path controllerConfig,
            Path brokerOneConfig,
            Path brokerTwoConfig,
            Path controllerFormatLog,
            Path brokerOneFormatLog,
            Path brokerTwoFormatLog,
            Path controllerServerLog,
            Path brokerOneServerLog,
            Path brokerTwoServerLog
    ) {
        String configured = System.getProperty("nereus.kafka.process.evidence.dir");
        if (configured == null || configured.isBlank()) {
            return;
        }
        Path target = Path.of(configured).toAbsolutePath().normalize();
        try {
            Files.createDirectories(target);
            copyIfPresent(
                    controllerConfig,
                    target.resolve("inflight-controller.properties"));
            copyIfPresent(
                    brokerOneConfig,
                    target.resolve("inflight-broker-one.properties"));
            copyIfPresent(
                    brokerTwoConfig,
                    target.resolve("inflight-broker-two.properties"));
            copyIfPresent(
                    controllerFormatLog,
                    target.resolve("inflight-controller-format.log"));
            copyIfPresent(
                    brokerOneFormatLog,
                    target.resolve("inflight-broker-one-format.log"));
            copyIfPresent(
                    brokerTwoFormatLog,
                    target.resolve("inflight-broker-two-format.log"));
            copyIfPresent(
                    controllerServerLog,
                    target.resolve("inflight-controller-server.log"));
            copyIfPresent(
                    brokerOneServerLog,
                    target.resolve("inflight-broker-one-server.log"));
            copyIfPresent(
                    brokerTwoServerLog,
                    target.resolve("inflight-broker-two-server.log"));
        } catch (IOException failure) {
            throw new AssertionError(
                    "failed to preserve in-flight takeover evidence under " + target,
                    failure);
        }
    }

    private static void preserveBookKeeperInFlightFailureEvidence(
            Path controllerConfig,
            Path brokerOneConfig,
            Path brokerTwoConfig,
            Path controllerFormatLog,
            Path brokerOneFormatLog,
            Path brokerTwoFormatLog,
            Path controllerServerLog,
            Path brokerOneServerLog,
            Path brokerTwoServerLog,
            Path agentInstalled,
            Path agentCaptured,
            Path agentApplied,
            Path agentRelease
    ) {
        String configured =
                System.getProperty(
                        "nereus.kafka.process.evidence.dir");
        if (configured == null || configured.isBlank()) {
            return;
        }
        Path target = Path.of(configured).toAbsolutePath().normalize();
        try {
            Files.createDirectories(target);
            copyIfPresent(
                    controllerConfig,
                    target.resolve(
                            "bookkeeper-inflight-controller.properties"));
            copyIfPresent(
                    brokerOneConfig,
                    target.resolve(
                            "bookkeeper-inflight-broker-one.properties"));
            copyIfPresent(
                    brokerTwoConfig,
                    target.resolve(
                            "bookkeeper-inflight-broker-two.properties"));
            copyIfPresent(
                    controllerFormatLog,
                    target.resolve(
                            "bookkeeper-inflight-controller-format.log"));
            copyIfPresent(
                    brokerOneFormatLog,
                    target.resolve(
                            "bookkeeper-inflight-broker-one-format.log"));
            copyIfPresent(
                    brokerTwoFormatLog,
                    target.resolve(
                            "bookkeeper-inflight-broker-two-format.log"));
            copyIfPresent(
                    controllerServerLog,
                    target.resolve(
                            "bookkeeper-inflight-controller-server.log"));
            copyIfPresent(
                    brokerOneServerLog,
                    target.resolve(
                            "bookkeeper-inflight-broker-one-server.log"));
            copyIfPresent(
                    brokerTwoServerLog,
                    target.resolve(
                            "bookkeeper-inflight-broker-two-server.log"));
            copyIfPresent(
                    agentInstalled,
                    target.resolve(
                            "bookkeeper-inflight-agent-installed"));
            copyIfPresent(
                    agentCaptured,
                    target.resolve(
                            "bookkeeper-inflight-agent-captured"));
            copyIfPresent(
                    agentApplied,
                    target.resolve(
                            "bookkeeper-inflight-agent-applied"));
            copyIfPresent(
                    agentRelease,
                    target.resolve(
                            "bookkeeper-inflight-agent-release"));
        } catch (IOException failure) {
            throw new AssertionError(
                    "failed to preserve BookKeeper in-flight takeover evidence under "
                            + target,
                    failure);
        }
    }

    private static void preserveBookKeeperTakeoverFailureEvidence(
            String profile,
            Path brokerOneConfig,
            Path brokerTwoConfig,
            Path brokerOneFormatLog,
            Path brokerTwoFormatLog,
            Path brokerOneServerLog,
            Path brokerTwoServerLog
    ) {
        String configured =
                System.getProperty("nereus.kafka.process.evidence.dir");
        if (configured == null || configured.isBlank()) {
            return;
        }
        Path target = Path.of(configured).toAbsolutePath().normalize();
        try {
            Files.createDirectories(target);
            copyIfPresent(
                    brokerOneConfig,
                    target.resolve(profile + "-one.properties"));
            copyIfPresent(
                    brokerTwoConfig,
                    target.resolve(profile + "-two.properties"));
            copyIfPresent(
                    brokerOneFormatLog,
                    target.resolve(profile + "-one-format.log"));
            copyIfPresent(
                    brokerTwoFormatLog,
                    target.resolve(profile + "-two-format.log"));
            copyIfPresent(
                    brokerOneServerLog,
                    target.resolve(profile + "-one-server.log"));
            copyIfPresent(
                    brokerTwoServerLog,
                    target.resolve(profile + "-two-server.log"));
        } catch (IOException failure) {
            throw new AssertionError(
                    "failed to preserve "
                            + profile
                            + " BookKeeper takeover evidence under "
                            + target,
                    failure);
        }
    }

    private static void clearFailureEvidence() throws IOException {
        String configured = System.getProperty("nereus.kafka.process.evidence.dir");
        if (configured == null || configured.isBlank()) {
            return;
        }
        Path target = Path.of(configured).toAbsolutePath().normalize();
        Files.deleteIfExists(target.resolve("server.properties"));
        Files.deleteIfExists(target.resolve("format.log"));
        Files.deleteIfExists(target.resolve("server-first.log"));
        Files.deleteIfExists(target.resolve("server-restart.log"));
        Files.deleteIfExists(target.resolve("server-interrupted-transaction.log"));
        Files.deleteIfExists(target.resolve("server-transaction-recovery.log"));
        Files.deleteIfExists(target.resolve("bookkeeper-server-first.log"));
        Files.deleteIfExists(target.resolve("bookkeeper-server-restart.log"));
        Files.deleteIfExists(target.resolve("object-async-server-first.log"));
        Files.deleteIfExists(target.resolve("object-async-server-restart.log"));
        Files.deleteIfExists(target.resolve("multi-broker-one.properties"));
        Files.deleteIfExists(target.resolve("multi-broker-two.properties"));
        Files.deleteIfExists(target.resolve("multi-broker-one-format.log"));
        Files.deleteIfExists(target.resolve("multi-broker-two-format.log"));
        Files.deleteIfExists(target.resolve("multi-broker-one-server.log"));
        Files.deleteIfExists(target.resolve("multi-broker-two-server.log"));
        for (int nodeId = 1; nodeId <= 3; nodeId++) {
            Files.deleteIfExists(
                    target.resolve(
                            "multi-controller-node-"
                                    + nodeId
                                    + ".properties"));
            Files.deleteIfExists(
                    target.resolve(
                            "multi-controller-node-"
                                    + nodeId
                                    + "-format.log"));
            Files.deleteIfExists(
                    target.resolve(
                            "multi-controller-node-"
                                    + nodeId
                                    + "-server.log"));
        }
        for (ActivationPublicationCut cut :
                ActivationPublicationCut.values()) {
            for (int nodeId = 1;
                    nodeId <= 4;
                    nodeId++) {
                String prefix =
                        "activation-cut-"
                                + cut.slug()
                                + "-node-"
                                + nodeId;
                Files.deleteIfExists(
                        target.resolve(
                                prefix
                                        + ".properties"));
                Files.deleteIfExists(
                        target.resolve(
                                prefix
                                        + "-format.log"));
                Files.deleteIfExists(
                        target.resolve(
                                prefix
                                        + "-server.log"));
            }
            for (int nodeId = 1;
                    nodeId <= 3;
                    nodeId++) {
                String prefix =
                        "activation-cut-"
                                + cut.slug()
                                + "-controller-"
                                + nodeId
                                + "-agent-";
                for (String marker :
                        List.of(
                                "arm",
                                "captured",
                                "blocked",
                                "applied",
                                "installed")) {
                    Files.deleteIfExists(
                            target.resolve(
                                    prefix
                                            + marker));
                }
            }
        }
        Files.deleteIfExists(target.resolve("inflight-controller.properties"));
        Files.deleteIfExists(target.resolve("inflight-broker-one.properties"));
        Files.deleteIfExists(target.resolve("inflight-broker-two.properties"));
        Files.deleteIfExists(target.resolve("inflight-controller-format.log"));
        Files.deleteIfExists(target.resolve("inflight-broker-one-format.log"));
        Files.deleteIfExists(target.resolve("inflight-broker-two-format.log"));
        Files.deleteIfExists(target.resolve("inflight-controller-server.log"));
        Files.deleteIfExists(target.resolve("inflight-broker-one-server.log"));
        Files.deleteIfExists(target.resolve("inflight-broker-two-server.log"));
        for (String file :
                List.of(
                        "bookkeeper-inflight-controller.properties",
                        "bookkeeper-inflight-broker-one.properties",
                        "bookkeeper-inflight-broker-two.properties",
                        "bookkeeper-inflight-controller-format.log",
                        "bookkeeper-inflight-broker-one-format.log",
                        "bookkeeper-inflight-broker-two-format.log",
                        "bookkeeper-inflight-controller-server.log",
                        "bookkeeper-inflight-broker-one-server.log",
                        "bookkeeper-inflight-broker-two-server.log",
                        "bookkeeper-inflight-agent-installed",
                        "bookkeeper-inflight-agent-captured",
                        "bookkeeper-inflight-agent-applied",
                        "bookkeeper-inflight-agent-release")) {
            Files.deleteIfExists(target.resolve(file));
        }
        for (String profile :
                List.of(
                        "wal-only",
                        "wal-async-object",
                        "wal-sync-object")) {
            Files.deleteIfExists(
                    target.resolve(profile + "-one.properties"));
            Files.deleteIfExists(
                    target.resolve(profile + "-two.properties"));
            Files.deleteIfExists(
                    target.resolve(profile + "-one-format.log"));
            Files.deleteIfExists(
                    target.resolve(profile + "-two-format.log"));
            Files.deleteIfExists(
                    target.resolve(profile + "-one-server.log"));
            Files.deleteIfExists(
                    target.resolve(profile + "-two-server.log"));
        }
    }

    private static void copyIfPresent(Path source, Path target) throws IOException {
        if (Files.exists(source)) {
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(false);
            return socket.getLocalPort();
        }
    }

    private static int differentFreePort(int... excluded) throws IOException {
        int candidate;
        boolean conflict;
        do {
            candidate = freePort();
            conflict = false;
            for (int port : excluded) {
                if (port == candidate) {
                    conflict = true;
                    break;
                }
            }
        } while (conflict);
        return candidate;
    }

    private static void createBucket(String bucket) {
        try (S3AsyncClient admin = s3Client()) {
            admin.createBucket(CreateBucketRequest.builder().bucket(bucket).build()).join();
        }
    }

    private static int objectCount(String bucket) {
        try (S3AsyncClient admin = s3Client()) {
            return admin.listObjectsV2(
                            ListObjectsV2Request.builder().bucket(bucket).build())
                    .join()
                    .keyCount();
        }
    }

    private static Set<String> walObjectKeys(String bucket) {
        try (S3AsyncClient admin = s3Client()) {
            return admin.listObjectsV2(
                            ListObjectsV2Request.builder()
                                    .bucket(bucket)
                                    .maxKeys(1_000)
                                    .build())
                    .join()
                    .contents()
                    .stream()
                    .map(value -> value.key())
                    .filter(value -> value.contains("/wal/"))
                    .collect(java.util.stream.Collectors.toUnmodifiableSet());
        }
    }

    private static void awaitProviderAppendStack(
            Process broker,
            PendingProduce pending,
            Path serverLog
    ) throws Exception {
        long deadline =
                System.nanoTime() + Duration.ofSeconds(45).toNanos();
        String latestThreadDump = "<thread dump not yet captured>";
        while (System.nanoTime() < deadline) {
            if (pending.future().isDone()) {
                Throwable failure = pending.awaitFailure();
                throw new AssertionError(
                        "old Produce completed before entering provider-backed stable append:\n"
                                + readLog(serverLog),
                        failure);
            }
            latestThreadDump = threadDump(broker, serverLog);
            if (latestThreadDump.contains("kafka.log.nereus.NereusUnifiedLog.appendStable")
                    && latestThreadDump.contains("java.util.concurrent.CompletableFuture.get")) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(
                "old Produce did not block inside provider-backed stable append before the deadline:\n"
                        + readLog(serverLog)
                        + "\nLast thread dump:\n"
                        + tail(latestThreadDump, 32_000));
    }

    private static String threadDump(
            Process broker,
            Path serverLog
    ) throws Exception {
        if (!broker.isAlive()) {
            throw new AssertionError(
                    "cannot inspect an exited Kafka process:\n"
                            + readLog(serverLog));
        }
        Path jcmd =
                Path.of(System.getProperty("java.home"), "bin", "jcmd");
        if (!Files.isExecutable(jcmd)) {
            throw new AssertionError(
                    "JDK jcmd is unavailable at " + jcmd);
        }
        Process command =
                new ProcessBuilder(
                                jcmd.toString(),
                                Long.toString(broker.pid()),
                                "Thread.print",
                                "-l")
                        .redirectErrorStream(true)
                        .start();
        String output =
                new String(
                        command.getInputStream().readAllBytes(),
                        StandardCharsets.UTF_8);
        if (!command.waitFor(10, TimeUnit.SECONDS)) {
            command.destroyForcibly();
            throw new AssertionError(
                    "timed out capturing Kafka thread dump for process "
                            + broker.pid());
        }
        if (command.exitValue() != 0) {
            throw new AssertionError(
                    "jcmd failed for Kafka process "
                            + broker.pid()
                            + ":\n"
                            + output);
        }
        return output;
    }

    private static String tail(String value, int maximumLength) {
        if (value.length() <= maximumLength) {
            return value;
        }
        return value.substring(value.length() - maximumLength);
    }

    private static void removeToxic(
            ToxiproxyContainer.ContainerProxy proxy,
            String name
    ) throws IOException {
        proxy.toxics().get(name).remove();
    }

    private static S3AsyncClient s3Client() {
        return S3AsyncClient.builder()
                .endpointOverride(
                        LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
                .region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                LOCALSTACK.getAccessKey(),
                                LOCALSTACK.getSecretKey())))
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build();
    }

    private static String readLog(Path log) {
        try {
            if (!Files.exists(log)) {
                return "<missing " + log + ">";
            }
            String content = Files.readString(log, StandardCharsets.UTF_8);
            int maximum = 128_000;
            if (content.length() <= maximum) {
                return content;
            }
            int half = maximum / 2;
            return content.substring(0, half)
                    + "\n<... middle of log omitted ...>\n"
                    + content.substring(content.length() - half);
        } catch (IOException failure) {
            return "<failed to read " + log + ": " + failure + ">";
        }
    }

    @FunctionalInterface
    private interface BrokerAssertions {
        void verify() throws Exception;
    }

    @FunctionalInterface
    private interface FirstBrokerAssertions {
        FirstBrokerAssertions NONE =
                (bootstrapServers,
                        partition,
                        topic,
                        serverLog) -> {
                };

        void verify(
                String bootstrapServers,
                TopicPartition partition,
                String topic,
                Path serverLog
        ) throws Exception;
    }

    private enum StopMode {
        NORMAL {
            @Override
            void stop(Process broker, Path serverLog) throws Exception {
                stopBroker(broker, serverLog);
            }
        },
        FORCE {
            @Override
            void stop(Process broker, Path serverLog) throws Exception {
                killBroker(broker, serverLog);
            }
        };

        abstract void stop(Process broker, Path serverLog) throws Exception;
    }

    private enum ActivationPublicationCut {
        PREPARED_BEFORE_PROVIDER(
                "prep-before",
                "createActivation",
                ActivationFaultPhase
                        .BEFORE_PROVIDER,
                ActivationDurableState
                        .ABSENT),
        PREPARED_APPLIED(
                "prep-applied",
                "createActivation",
                ActivationFaultPhase
                        .AFTER_PROVIDER,
                ActivationDurableState
                        .PREPARED),
        ACTIVE_BEFORE_PROVIDER(
                "active-before",
                "compareAndSetActivation",
                ActivationFaultPhase
                        .BEFORE_PROVIDER,
                ActivationDurableState
                        .PREPARED),
        ACTIVE_APPLIED(
                "active-applied",
                "compareAndSetActivation",
                ActivationFaultPhase
                        .AFTER_PROVIDER,
                ActivationDurableState
                        .ACTIVE);

        private final String slug;
        private final String operation;
        private final ActivationFaultPhase phase;
        private final ActivationDurableState durableState;

        ActivationPublicationCut(
                String slug,
                String operation,
                ActivationFaultPhase phase,
                ActivationDurableState durableState
        ) {
            this.slug = slug;
            this.operation = operation;
            this.phase = phase;
            this.durableState = durableState;
        }

        private String slug() {
            return slug;
        }

        private String operation() {
            return operation;
        }

        private ActivationFaultPhase phase() {
            return phase;
        }

        private ActivationDurableState durableState() {
            return durableState;
        }
    }

    private enum ActivationFaultPhase {
        BEFORE_PROVIDER("before-provider"),
        AFTER_PROVIDER("after-provider");

        private final String agentValue;

        ActivationFaultPhase(
                String agentValue
        ) {
            this.agentValue = agentValue;
        }

        private String agentValue() {
            return agentValue;
        }
    }

    private enum ActivationDurableState {
        ABSENT(null),
        PREPARED(
                KafkaStorageActivationLifecycle
                        .PREPARED),
        ACTIVE(
                KafkaStorageActivationLifecycle
                        .ACTIVE);

        private final KafkaStorageActivationLifecycle lifecycle;

        ActivationDurableState(
                KafkaStorageActivationLifecycle lifecycle
        ) {
            this.lifecycle = lifecycle;
        }

        private KafkaStorageActivationLifecycle lifecycle() {
            if (lifecycle == null) {
                throw new IllegalStateException(
                        "ABSENT activation has no lifecycle");
            }
            return lifecycle;
        }
    }

    private enum KafkaProcessRole {
        BROKER("broker", true, false),
        CONTROLLER("controller", false, true),
        COMBINED("broker,controller", true, true);

        private final String configurationValue;
        private final boolean broker;
        private final boolean controller;

        KafkaProcessRole(
                String configurationValue,
                boolean broker,
                boolean controller
        ) {
            this.configurationValue = configurationValue;
            this.broker = broker;
            this.controller = controller;
        }

        private String configurationValue() {
            return configurationValue;
        }

        private boolean hasBroker() {
            return broker;
        }

        private String listeners(
                int brokerPort,
                int controllerPort
        ) {
            if (broker && controller) {
                return "PLAINTEXT://127.0.0.1:"
                        + brokerPort
                        + ",CONTROLLER://127.0.0.1:"
                        + controllerPort;
            }
            if (broker) {
                return "PLAINTEXT://127.0.0.1:"
                        + brokerPort;
            }
            return "CONTROLLER://127.0.0.1:"
                    + controllerPort;
        }
    }

    private record BookKeeperProcessConfiguration(
            String metadataServiceUri,
            String deploymentId,
            String clusterAlias,
            String providerScopeSha256,
            int ledgerIdPrefixBits,
            long ledgerIdPrefixValue,
            String ledgerIdReservationId,
            Path passwordFile,
            String passwordVersion,
            long readinessEpoch,
            String readinessSha256,
            int persistentBrokerCount) {
    }

    private record BookKeeperTakeoverProfile(
            String storageProfile,
            String fixtureToken,
            int authoritySeed,
            boolean requireMaterializedObject) {
    }

    private record BookKeeperInFlightEvidence(
            StreamId streamId,
            String reservationId,
            long ledgerId,
            long entryId) {
    }

    private record ControllerQuorumEvidence(
            int leaderId,
            long leaderEpoch,
            long highWatermark) {
    }

    private record KafkaActivationEvidence(
            KafkaStorageProtocolActivationRecord activation,
            KafkaStorageReadinessRecord readiness) {
    }

    private record KafkaActivationCutEvidence(
            Optional<KafkaStorageProtocolActivationRecord> activation,
            KafkaStorageReadinessRecord readiness) {
    }

    private record ActivationAgentMarkers(
            Path arm,
            Path captured,
            Path blocked,
            Path applied,
            Path installed) {
    }

    private record PendingProduce(
            KafkaProducer<byte[], byte[]> producer,
            Future<RecordMetadata> future
    ) implements AutoCloseable {
        private Throwable awaitFailure() throws Exception {
            try {
                RecordMetadata unexpected =
                        future.get(
                                CLIENT_TIMEOUT.toSeconds(),
                                TimeUnit.SECONDS);
                throw new AssertionError(
                        "old-leader Produce unexpectedly completed at offset "
                                + unexpected.offset());
            } catch (ExecutionException failure) {
                return failure.getCause() == null
                        ? failure
                        : failure.getCause();
            } catch (TimeoutException failure) {
                throw new AssertionError(
                        "old-leader Produce did not complete after broker resume",
                        failure);
            }
        }

        @Override
        public void close() {
            producer.close(Duration.ZERO);
        }
    }

    private record OpenTransaction(
            KafkaProducer<byte[], byte[]> producer,
            RecordMetadata metadata
    ) implements AutoCloseable {
        @Override
        public void close() {
            producer.close(Duration.ZERO);
        }
    }
}
