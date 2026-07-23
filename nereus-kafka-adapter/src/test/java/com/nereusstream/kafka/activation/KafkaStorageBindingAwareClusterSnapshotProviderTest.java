/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.kafka.activation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.metadata.oxia.KafkaBrokerIdentity;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.KafkaPartitionMetadataStore;
import com.nereusstream.metadata.oxia.KafkaPartitionScanPage;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionRegistry;
import com.nereusstream.metadata.oxia.records.KafkaPartitionBindingRecord;
import com.nereusstream.metadata.oxia.records.KafkaPartitionLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaPartitionRegistryRecord;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class KafkaStorageBindingAwareClusterSnapshotProviderTest {
    private static final KafkaStorageClusterSnapshot EMPTY = new KafkaStorageClusterSnapshot(
            "kafka-a",
            17,
            1,
            List.of(new KafkaBrokerIdentity(1, 9)),
            false,
            false,
            false);

    @Test
    void provesAllRegistryShardsEmpty() {
        StubStore store = new StubStore(-1);
        KafkaStorageBindingAwareClusterSnapshotProvider provider =
                new KafkaStorageBindingAwareClusterSnapshotProvider(
                        () -> CompletableFuture.completedFuture(EMPTY), store);

        KafkaStorageClusterSnapshot snapshot =
                provider.currentSnapshot().toCompletableFuture().join();

        assertSame(EMPTY, snapshot);
        assertFalse(snapshot.bindingsPresent());
        assertEquals(64, store.scans.get());
    }

    @Test
    void reportsBindingHintFromAnyShard() {
        StubStore store = new StubStore(37);
        KafkaStorageBindingAwareClusterSnapshotProvider provider =
                new KafkaStorageBindingAwareClusterSnapshotProvider(
                        () -> CompletableFuture.completedFuture(EMPTY), store);

        KafkaStorageClusterSnapshot snapshot =
                provider.currentSnapshot().toCompletableFuture().join();

        assertTrue(snapshot.bindingsPresent());
        assertEquals(64, store.scans.get());
        assertEquals(EMPTY.metadataOffset(), snapshot.metadataOffset());
        assertEquals(EMPTY.brokers(), snapshot.brokers());
    }

    @Test
    void preservesAlreadyAuthoritativeBindingFactWithoutScanning() {
        KafkaStorageClusterSnapshot supplied = new KafkaStorageClusterSnapshot(
                "kafka-a",
                18,
                1,
                EMPTY.brokers(),
                true,
                true,
                true);
        StubStore store = new StubStore(0);
        KafkaStorageBindingAwareClusterSnapshotProvider provider =
                new KafkaStorageBindingAwareClusterSnapshotProvider(
                        () -> CompletableFuture.completedFuture(supplied), store);

        assertSame(supplied, provider.currentSnapshot().toCompletableFuture().join());
        assertEquals(0, store.scans.get());
    }

    private static final class StubStore implements KafkaPartitionMetadataStore {
        private final int populatedShard;
        private final AtomicInteger scans = new AtomicInteger();

        private StubStore(int populatedShard) {
            this.populatedShard = populatedShard;
        }

        @Override
        public CompletableFuture<KafkaPartitionScanPage> scanRegistry(
                int shard, Optional<String> continuation, int limit) {
            scans.incrementAndGet();
            if (shard == populatedShard) {
                byte[] rootSha256 = new byte[32];
                rootSha256[0] = 1;
                KafkaPartitionRegistryRecord record = new KafkaPartitionRegistryRecord(
                        1,
                        "kafka-a",
                        "AQIDBAUGBwgJCgsMDQ4PEA",
                        0,
                        "binding-root",
                        rootSha256,
                        KafkaPartitionLifecycle.ACTIVE.wireId(),
                        1,
                        2,
                        3);
                return CompletableFuture.completedFuture(new KafkaPartitionScanPage(
                        List.of(new VersionedKafkaPartitionRegistry(
                                "key",
                                record,
                                3,
                                new Checksum(
                                        ChecksumType.SHA256,
                                        "01" + "00".repeat(31)))),
                        Optional.empty()));
            }
            return CompletableFuture.completedFuture(
                    new KafkaPartitionScanPage(List.of(), Optional.empty()));
        }

        @Override
        public CompletableFuture<Optional<VersionedKafkaPartitionBinding>> get(KafkaPartitionId id) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<VersionedKafkaPartitionBinding> putCreatingIfAbsent(
                KafkaPartitionBindingRecord value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<VersionedKafkaPartitionBinding> compareAndSet(
                VersionedKafkaPartitionBinding expected,
                KafkaPartitionBindingRecord update) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> putRegistryHint(KafkaPartitionRegistryRecord value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() { }
    }
}
