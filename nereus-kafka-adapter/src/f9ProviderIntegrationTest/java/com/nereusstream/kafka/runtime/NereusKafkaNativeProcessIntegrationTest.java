/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.oxia.testcontainers.OxiaContainer;
import java.io.IOException;
import java.io.Writer;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
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

    private void writeConfiguration(
            Path config,
            int brokerPort,
            int controllerPort,
            String bucket,
            Path logDirectory,
            Path metadataDirectory,
            Path cacheDirectory
    ) throws IOException {
        Files.createDirectories(logDirectory);
        Files.createDirectories(metadataDirectory);
        Files.createDirectories(cacheDirectory);
        Properties properties = new Properties();
        properties.setProperty("process.roles", "broker,controller");
        properties.setProperty("node.id", "1");
        properties.setProperty("controller.quorum.voters", "1@127.0.0.1:" + controllerPort);
        properties.setProperty(
                "listeners",
                "PLAINTEXT://127.0.0.1:" + brokerPort
                        + ",CONTROLLER://127.0.0.1:" + controllerPort);
        properties.setProperty(
                "advertised.listeners",
                "PLAINTEXT://127.0.0.1:" + brokerPort);
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
        properties.setProperty("nereus.kafka.storage.cluster", "f9-process-" + UUID.randomUUID());
        properties.setProperty("nereus.kafka.storage.profile", "OBJECT_WAL_SYNC_OBJECT");
        properties.setProperty(
                "nereus.kafka.storage.oxia.service.address",
                OXIA.getServiceAddress());
        properties.setProperty("nereus.kafka.storage.oxia.namespace", "default");
        properties.setProperty("nereus.kafka.storage.object.provider", "s3");
        properties.setProperty("nereus.kafka.storage.object.bucket", bucket);
        properties.setProperty(
                "nereus.kafka.storage.object.endpoint",
                LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        properties.setProperty(
                "nereus.kafka.storage.object.region",
                LOCALSTACK.getRegion());
        properties.setProperty("nereus.kafka.storage.object.path.style.access", "true");
        properties.setProperty("nereus.kafka.storage.cache.dir", cacheDirectory.toString());
        properties.setProperty("nereus.kafka.storage.compaction.enabled", "true");
        properties.setProperty("nereus.kafka.storage.append.executor.threads", "2");
        properties.setProperty("nereus.kafka.storage.fetch.executor.threads", "2");
        properties.setProperty("nereus.kafka.storage.lifecycle.executor.threads", "2");
        properties.setProperty("nereus.kafka.storage.recovery.executor.threads", "2");
        properties.setProperty("nereus.kafka.storage.readiness.timeout.ms", "90000");
        properties.setProperty("nereus.kafka.storage.capability.heartbeat.ms", "1000");
        properties.setProperty("nereus.kafka.storage.capability.expiry.ms", "30000");
        properties.setProperty("nereus.kafka.storage.shutdown.drain.timeout.ms", "30000");
        properties.setProperty("nereus.kafka.storage.shutdown.checkpoint.timeout.ms", "30000");
        try (Writer writer = Files.newBufferedWriter(config, StandardCharsets.UTF_8)) {
            properties.store(writer, "F9 native Kafka provider-backed process gate");
        }
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

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception exact) {
            throw exact;
        }
        if (failure instanceof AssertionError exact) {
            throw exact;
        }
        throw new AssertionError("unexpected Kafka process failure", failure);
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

    private static Process start(
            List<String> command,
            Path workingDirectory,
            Path output
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

    private static int differentFreePort(int first) throws IOException {
        int candidate;
        do {
            candidate = freePort();
        } while (candidate == first);
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
