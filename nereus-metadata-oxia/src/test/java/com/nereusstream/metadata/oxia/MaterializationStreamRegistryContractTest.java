/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static com.nereusstream.metadata.oxia.F4MetadataTestValues.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.StreamId;
import com.nereusstream.metadata.oxia.records.MaterializationStreamRegistrationRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class MaterializationStreamRegistryContractTest {
    @Test
    void coversAll64ShardsWithStrictPaginationIdentityAndConditionalLifecycle() {
        OxiaJavaGenerationMetadataStore store = store();
        F4Keyspace keys = new F4Keyspace(CLUSTER);
        Map<Integer, StreamId> streams = oneStreamPerShard(keys);
        Map<Integer, VersionedMaterializationStreamRegistration> created = new LinkedHashMap<>();

        streams.forEach((shard, stream) -> {
            MaterializationStreamRegistrationRecord registration =
                    F4MetadataTestValues.registration(stream.value(), shard);
            VersionedMaterializationStreamRegistration value = store.createOrVerifyStreamRegistration(
                    CLUSTER, registration).join();
            assertThat(keys.materializationRegistryShard(stream)).isEqualTo(shard);
            assertThat(store.createOrVerifyStreamRegistration(CLUSTER, registration).join())
                    .isEqualTo(value);
            created.put(shard, value);
        });

        F4ScanToken shardZeroToken = null;
        for (int shard = 0; shard < 64; shard++) {
            StreamRegistrationScanPage first = store.scanStreamRegistrations(
                    CLUSTER, shard, Optional.empty(), 1).join();
            assertThat(first.values()).containsExactly(created.get(shard));
            assertThat(first.continuation()).isPresent();
            StreamRegistrationScanPage terminal = store.scanStreamRegistrations(
                    CLUSTER, shard, first.continuation(), 1).join();
            assertThat(terminal.values()).isEmpty();
            assertThat(terminal.continuation()).isEmpty();
            if (shard == 0) {
                shardZeroToken = first.continuation().orElseThrow();
            }
        }

        F4ScanToken wrongScope = shardZeroToken;
        assertThatThrownBy(() -> store.scanStreamRegistrations(
                        CLUSTER, 1, Optional.of(wrongScope), 1).join())
                .isInstanceOfAny(IllegalArgumentException.class, CompletionException.class);

        StreamId stream = streams.get(0);
        VersionedMaterializationStreamRegistration original = created.get(0);
        MaterializationStreamRegistrationRecord advanced = new MaterializationStreamRegistrationRecord(
                1,
                stream.value(),
                original.value().projectionRef(),
                original.value().projectionIdentitySha256(),
                original.value().storageProfile(),
                original.value().registeredAtMillis(),
                999,
                1_000,
                0);
        VersionedMaterializationStreamRegistration updated = store.compareAndSetStreamRegistration(
                CLUSTER, advanced, original.metadataVersion()).join();
        assertThat(updated.value().registeredAtMillis()).isEqualTo(original.value().registeredAtMillis());
        assertThat(updated.value().lastHintCommitVersion()).isEqualTo(999);

        MaterializationStreamRegistrationRecord sameIdentityDifferentHint =
                F4MetadataTestValues.registration(stream.value(), 1);
        assertThat(store.createOrVerifyStreamRegistration(
                        CLUSTER, sameIdentityDifferentHint).join())
                .isEqualTo(updated);

        MaterializationStreamRegistrationRecord collision = new MaterializationStreamRegistrationRecord(
                1,
                stream.value(),
                "different-projection",
                "f".repeat(64),
                original.value().storageProfile(),
                100,
                0,
                100,
                0);
        assertThatThrownBy(() -> store.createOrVerifyStreamRegistration(CLUSTER, collision).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOf(com.nereusstream.api.NereusException.class));
        assertThatThrownBy(() -> store.deleteStreamRegistration(
                        CLUSTER, stream, original.metadataVersion()).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);

        created.forEach((shard, value) -> {
            StreamId exactStream = streams.get(shard);
            long version = shard == 0 ? updated.metadataVersion() : value.metadataVersion();
            store.deleteStreamRegistration(CLUSTER, exactStream, version).join();
        });
    }

    private static Map<Integer, StreamId> oneStreamPerShard(F4Keyspace keys) {
        Map<Integer, StreamId> result = new LinkedHashMap<>();
        for (int candidate = 0; candidate < 100_000 && result.size() < 64; candidate++) {
            StreamId stream = new StreamId("registry-stream-" + candidate);
            result.putIfAbsent(keys.materializationRegistryShard(stream), stream);
        }
        assertThat(result).hasSize(64);
        return result;
    }

    private static OxiaJavaGenerationMetadataStore store() {
        return new OxiaJavaGenerationMetadataStore(
                new PartitionedOxiaClient(new InMemoryPartitionedOxiaBackend()),
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
    }

    private static Throwable unwrap(Throwable supplied) {
        Throwable current = supplied;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
