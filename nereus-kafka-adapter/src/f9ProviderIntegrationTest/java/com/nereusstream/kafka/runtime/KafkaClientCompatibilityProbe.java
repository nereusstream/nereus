/* Licensed under the Apache License, Version 2.0 */

package com.nereusstream.kafka.runtime;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
import org.apache.kafka.common.utils.AppInfoParser;

/**
 * Standalone binary-compatibility probe.
 *
 * <p>The parent process deliberately launches this class in a new JVM whose classpath contains exactly one selected
 * kafka-clients version. Keep this class limited to public client APIs available at the F9 support floor.
 */
public final class KafkaClientCompatibilityProbe {
    private static final Duration CLIENT_TIMEOUT = Duration.ofSeconds(60);

    private KafkaClientCompatibilityProbe() {}

    public static void main(String[] arguments) throws Exception {
        if (arguments.length != 3) {
            throw new IllegalArgumentException(
                    "expected arguments: <expected-client-version> <bootstrap-servers> <identity>");
        }
        String expectedVersion = requireToken(arguments[0], "expected client version");
        String bootstrapServers = requireToken(arguments[1], "bootstrap servers");
        String identity = requireToken(arguments[2], "identity");
        String actualVersion = AppInfoParser.getVersion();
        require(
                expectedVersion.equals(actualVersion),
                "selected classpath resolved kafka-clients " + actualVersion + " instead of " + expectedVersion);

        String topic = "f9-compat-" + identity;
        String groupId = "f9-compat-group-" + identity;
        String transactionalId = "f9-compat-txn-" + identity;
        TopicPartition partition = new TopicPartition(topic, 0);
        byte[] plainValue = ("plain-" + expectedVersion).getBytes(StandardCharsets.UTF_8);
        byte[] committedValue = ("committed-" + expectedVersion).getBytes(StandardCharsets.UTF_8);
        byte[] abortedValue = ("aborted-" + expectedVersion).getBytes(StandardCharsets.UTF_8);

        try (Admin admin = Admin.create(adminProperties(bootstrapServers))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all()
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            var description = admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                    .get(topic);
            require(description != null, "Admin describeTopics omitted " + topic);
            require(description.partitions().size() == 1, "Admin observed a non-singleton topic");
            require(
                    description.partitions().get(0).replicas().size() == 1,
                    "Admin observed a non-singleton replica set");
            require(description.partitions().get(0).isr().size() == 1, "Admin observed a non-singleton ISR");

            RecordMetadata plain = produce(
                    bootstrapServers,
                    topic,
                    ("plain-key-" + expectedVersion).getBytes(StandardCharsets.UTF_8),
                    plainValue);
            require(plain.partition() == 0, "plain Produce returned the wrong partition");
            require(plain.offset() == 0L, "plain Produce returned offset " + plain.offset());

            ConsumerRecord<byte[], byte[]> fetched = fetch(bootstrapServers, partition, 0L, "read_uncommitted");
            require(fetched.offset() == 0L, "Fetch returned offset " + fetched.offset());
            require(java.util.Arrays.equals(fetched.value(), plainValue), "Fetch returned different plain bytes");

            consumeAndCommitGroup(bootstrapServers, groupId, topic, partition, plainValue);
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            require(
                    committed.containsKey(partition) && committed.get(partition).offset() == 1L,
                    "group offset did not converge to 1: " + committed);
            require(
                    admin.describeConsumerGroups(List.of(groupId))
                            .all()
                            .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)
                            .containsKey(groupId),
                    "Admin describeConsumerGroups omitted " + groupId);

            RecordMetadata transaction = transactionalProduce(
                    bootstrapServers,
                    transactionalId,
                    topic,
                    ("committed-key-" + expectedVersion).getBytes(StandardCharsets.UTF_8),
                    committedValue,
                    true);
            require(
                    transaction.offset() == 1L,
                    "committed transactional Produce returned offset " + transaction.offset());
            RecordMetadata aborted = transactionalProduce(
                    bootstrapServers,
                    transactionalId,
                    topic,
                    ("aborted-key-" + expectedVersion).getBytes(StandardCharsets.UTF_8),
                    abortedValue,
                    false);
            require(aborted.offset() == 3L, "aborted transactional Produce returned offset " + aborted.offset());

            List<ConsumerRecord<byte[], byte[]>> visible = fetchReadCommitted(bootstrapServers, partition, 5L);
            require(
                    visible.stream().map(ConsumerRecord::offset).toList().equals(List.of(0L, 1L)),
                    "READ_COMMITTED exposed unexpected offsets "
                            + visible.stream().map(ConsumerRecord::offset).toList());
            require(
                    java.util.Arrays.equals(visible.get(0).value(), plainValue),
                    "READ_COMMITTED changed the plain value");
            require(
                    java.util.Arrays.equals(visible.get(1).value(), committedValue),
                    "READ_COMMITTED changed the committed transactional value");
            require(
                    visible.stream().noneMatch(record -> java.util.Arrays.equals(record.value(), abortedValue)),
                    "READ_COMMITTED exposed aborted transactional data");

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest = admin.listOffsets(
                            Map.of(partition, OffsetSpec.earliest()))
                    .all()
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latest = admin.listOffsets(
                            Map.of(partition, OffsetSpec.latest()))
                    .all()
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            require(earliest.get(partition).offset() == 0L, "earliest offset was not 0");
            require(latest.get(partition).offset() == 5L, "latest offset was not 5");
        }

        System.out.println("COMPATIBILITY_PASS version="
                + actualVersion
                + " operations=admin,produce,fetch,group,transaction"
                + " earliest=0 latest=5 committedGroupOffset=1"
                + " visibleOffsets=0,1");
    }

    private static RecordMetadata produce(String bootstrapServers, String topic, byte[] key, byte[] value)
            throws Exception {
        Properties properties = producerProperties(bootstrapServers);
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "f9-compat-plain");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            return producer.send(new ProducerRecord<>(topic, 0, key, value))
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        }
    }

    private static RecordMetadata transactionalProduce(
            String bootstrapServers, String transactionalId, String topic, byte[] key, byte[] value, boolean commit)
            throws Exception {
        Properties properties = producerProperties(bootstrapServers);
        properties.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        properties.setProperty(
                ProducerConfig.CLIENT_ID_CONFIG, "f9-compat-transaction-" + (commit ? "commit" : "abort"));
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            producer.initTransactions();
            producer.beginTransaction();
            RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, 0, key, value))
                    .get(CLIENT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            if (commit) {
                producer.commitTransaction();
            } else {
                producer.abortTransaction();
            }
            return metadata;
        }
    }

    private static void consumeAndCommitGroup(
            String bootstrapServers, String groupId, String topic, TopicPartition partition, byte[] expectedValue) {
        Properties properties = consumerProperties(bootstrapServers, groupId, "read_committed");
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + CLIENT_TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (record.partition() == partition.partition() && record.offset() == 0L) {
                        require(
                                java.util.Arrays.equals(record.value(), expectedValue),
                                "group Fetch returned different bytes");
                        consumer.commitSync(Map.of(partition, new OffsetAndMetadata(1L)), CLIENT_TIMEOUT);
                        return;
                    }
                }
            }
        }
        throw new AssertionError("group consumer did not fetch offset 0");
    }

    private static ConsumerRecord<byte[], byte[]> fetch(
            String bootstrapServers, TopicPartition partition, long offset, String isolationLevel) {
        Properties properties =
                consumerProperties(bootstrapServers, "f9-compat-direct-" + System.nanoTime(), isolationLevel);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seek(partition, offset);
            long deadline = System.nanoTime() + CLIENT_TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(250));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }
        throw new AssertionError("direct Fetch timed out at offset " + offset);
    }

    private static List<ConsumerRecord<byte[], byte[]>> fetchReadCommitted(
            String bootstrapServers, TopicPartition partition, long expectedEndOffset) {
        Properties properties =
                consumerProperties(bootstrapServers, "f9-compat-read-committed-" + System.nanoTime(), "read_committed");
        List<ConsumerRecord<byte[], byte[]>> visible = new ArrayList<>();
        Set<Long> observed = new LinkedHashSet<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seek(partition, 0L);
            long deadline = System.nanoTime() + CLIENT_TIMEOUT.toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (observed.add(record.offset())) {
                        visible.add(record);
                    }
                }
                long end =
                        consumer.endOffsets(Set.of(partition), CLIENT_TIMEOUT).get(partition);
                if (end == expectedEndOffset && consumer.position(partition) >= expectedEndOffset) {
                    return visible;
                }
            }
        }
        throw new AssertionError(
                "READ_COMMITTED did not reach end offset " + expectedEndOffset + "; visible=" + observed);
    }

    private static Properties producerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
        return properties;
    }

    private static Properties consumerProperties(String bootstrapServers, String groupId, String isolationLevel) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        properties.setProperty(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
        return properties;
    }

    private static Properties adminProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        properties.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000");
        return properties;
    }

    private static String requireToken(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
