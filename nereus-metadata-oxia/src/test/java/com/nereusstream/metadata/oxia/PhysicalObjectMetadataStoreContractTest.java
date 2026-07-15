/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import static com.nereusstream.metadata.oxia.F4MetadataTestValues.CLUSTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKey;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

class PhysicalObjectMetadataStoreContractTest {
    @Test
    void rootLeaseAndProtectionShareExactVersionedCreateCasScanContracts() {
        OxiaJavaPhysicalObjectMetadataStore store = store();
        PhysicalObjectRootRecord active = F4MetadataTestValues.physicalRoot(PhysicalObjectLifecycle.ACTIVE);
        ObjectKeyHash object = new ObjectKeyHash(active.objectKeyHash());

        VersionedPhysicalObjectRoot root = store.createRoot(CLUSTER, active).join();
        assertThat(store.createRoot(CLUSTER, active).join()).isEqualTo(root);
        assertThatThrownBy(() -> store.createRoot(CLUSTER, withLength(active, 129)).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOf(com.nereusstream.api.NereusException.class));

        ObjectReaderLeaseRecord leaseRecord = F4MetadataTestValues.readerLease();
        VersionedReaderLease lease = store.createOrCompareReaderLease(CLUSTER, leaseRecord).join();
        assertThat(store.createOrCompareReaderLease(CLUSTER, leaseRecord).join()).isEqualTo(lease);
        assertThatThrownBy(() -> store.createOrCompareReaderLease(
                        CLUSTER, withLeaseId(leaseRecord, "f".repeat(26))).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOf(com.nereusstream.api.NereusException.class));
        ObjectReaderLeaseRecord renewedRecord = new ObjectReaderLeaseRecord(
                1,
                leaseRecord.objectKeyHash(),
                leaseRecord.processRunId(),
                leaseRecord.leaseId(),
                leaseRecord.rootLifecycleEpoch(),
                leaseRecord.acquiredAtMillis(),
                400,
                350,
                1,
                0);
        VersionedReaderLease renewed = store.compareAndSetReaderLease(
                CLUSTER, renewedRecord, lease.metadataVersion()).join();
        assertThatThrownBy(() -> store.compareAndSetReaderLease(
                        CLUSTER, renewedRecord, lease.metadataVersion()).join())
                .hasCauseInstanceOf(F4MetadataConditionFailedException.class);
        ReaderLeaseScanPage leasePage = store.scanReaderLeases(
                CLUSTER, object, Optional.empty(), 1).join();
        assertThat(leasePage.values()).containsExactly(renewed);
        assertThat(store.scanReaderLeases(
                        CLUSTER, object, leasePage.continuation(), 1).join().values())
                .isEmpty();

        ObjectProtectionRecord protectionRecord = F4MetadataTestValues.protection(
                ObjectProtectionType.VISIBLE_GENERATION);
        VersionedObjectProtection protection = store.createProtection(CLUSTER, protectionRecord).join();
        assertThat(store.createProtection(CLUSTER, protectionRecord).join()).isEqualTo(protection);
        assertThatThrownBy(() -> store.createProtection(
                        CLUSTER, withOwner(protectionRecord, "/owners/conflict")).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOf(com.nereusstream.api.NereusException.class));
        ObjectProtectionRecord transferredRecord = withOwner(protectionRecord, "/owners/transferred");
        VersionedObjectProtection transferred = store.compareAndSetProtection(
                CLUSTER, transferredRecord, protection.metadataVersion()).join();
        ObjectProtectionScanPage protectionPage = store.scanProtections(
                CLUSTER, object, Optional.empty(), 1).join();
        assertThat(protectionPage.values()).containsExactly(transferred);
        assertThat(store.scanProtections(
                        CLUSTER, object, protectionPage.continuation(), 1).join().values())
                .isEmpty();

        PhysicalObjectRootRecord marked = rootState(active, PhysicalObjectLifecycle.MARKED, 2, false);
        VersionedPhysicalObjectRoot markedRoot = store.compareAndSetRoot(
                CLUSTER, marked, root.metadataVersion()).join();
        assertThat(markedRoot.value().lifecycleEpoch()).isEqualTo(2);
        assertThatThrownBy(() -> store.compareAndSetRoot(
                        CLUSTER,
                        rootState(active, PhysicalObjectLifecycle.DELETED, 2, false),
                        markedRoot.metadataVersion()).join())
                .satisfies(error -> assertThat(unwrap(error))
                        .isInstanceOf(com.nereusstream.api.NereusException.class));

        int shard = new F4Keyspace(CLUSTER).physicalObjectShard(object);
        PhysicalObjectRootScanPage roots = store.scanRoots(CLUSTER, shard, Optional.empty(), 10).join();
        assertThat(roots.values()).containsExactly(markedRoot);

        store.deleteProtection(
                CLUSTER,
                new ObjectProtectionIdentity(
                        object, ObjectProtectionType.VISIBLE_GENERATION, protectionRecord.referenceId()),
                transferred.metadataVersion()).join();
        store.deleteReaderLease(
                CLUSTER, object, leaseRecord.processRunId(), renewed.metadataVersion()).join();
    }

    @Test
    void enumeratesOneRootInEveryFirstHashByteShardWithoutLeakingChildRecords() {
        OxiaJavaPhysicalObjectMetadataStore store = store();
        F4Keyspace keys = new F4Keyspace(CLUSTER);
        Map<Integer, PhysicalObjectRootRecord> roots = oneRootPerShard(keys);

        roots.values().forEach(root -> store.createRoot(CLUSTER, root).join());

        for (int shard = 0; shard < 256; shard++) {
            PhysicalObjectRootScanPage page = store.scanRoots(
                    CLUSTER, shard, Optional.empty(), 2).join();
            assertThat(page.values())
                    .singleElement()
                    .extracting(value -> value.value().objectKeyHash())
                    .isEqualTo(roots.get(shard).objectKeyHash());
            assertThat(page.continuation()).isEmpty();
        }
    }

    static PhysicalObjectRootRecord rootState(
            PhysicalObjectRootRecord identity,
            PhysicalObjectLifecycle lifecycle,
            long epoch,
            boolean tombstone) {
        boolean gc = lifecycle == PhysicalObjectLifecycle.MARKED
                || lifecycle == PhysicalObjectLifecycle.DELETING
                || lifecycle == PhysicalObjectLifecycle.DELETED;
        boolean deleting = lifecycle == PhysicalObjectLifecycle.DELETING
                || lifecycle == PhysicalObjectLifecycle.DELETED;
        boolean deleted = lifecycle == PhysicalObjectLifecycle.DELETED;
        return new PhysicalObjectRootRecord(
                1,
                identity.objectKeyHash(),
                identity.objectKey(),
                identity.objectId(),
                identity.objectKindId(),
                identity.objectLength(),
                identity.storageChecksumType(),
                identity.storageChecksumValue(),
                identity.contentSha256(),
                identity.etag(),
                lifecycle,
                epoch,
                identity.createdAtMillis(),
                identity.orphanNotBeforeMillis(),
                gc ? F4MetadataTestValues.ATTEMPT : "",
                gc ? F4MetadataTestValues.HASH_E : "",
                gc ? 300 : 0,
                gc ? 400 : 0,
                deleting ? 400 : 0,
                deleted ? 500 : 0,
                tombstone ? 600 : 0,
                tombstone ? F4MetadataTestValues.HASH_A : "",
                lifecycle == PhysicalObjectLifecycle.QUARANTINED ? "identity mismatch" : "",
                0);
    }

    private static Map<Integer, PhysicalObjectRootRecord> oneRootPerShard(F4Keyspace keys) {
        Map<Integer, PhysicalObjectRootRecord> result = new LinkedHashMap<>();
        for (int candidate = 0; candidate < 100_000 && result.size() < 256; candidate++) {
            ObjectKey key = new ObjectKey("objects/f4/shards/object-" + candidate);
            ObjectKeyHash hash = ObjectKeyHash.from(key);
            int shard = keys.physicalObjectShard(hash);
            result.putIfAbsent(shard, activeRoot(key, hash));
        }
        assertThat(result).hasSize(256);
        return result;
    }

    private static PhysicalObjectRootRecord activeRoot(ObjectKey key, ObjectKeyHash hash) {
        return new PhysicalObjectRootRecord(
                1, hash.value(), key.value(), "", 2, 1, ChecksumType.CRC32C.name(), "01020304", "", "",
                PhysicalObjectLifecycle.ACTIVE, 1, 100, 200, "", "", 0, 0, 0, 0, 0, "", "", 0);
    }

    private static PhysicalObjectRootRecord withLength(PhysicalObjectRootRecord value, long length) {
        return new PhysicalObjectRootRecord(
                value.schemaVersion(), value.objectKeyHash(), value.objectKey(), value.objectId(),
                value.objectKindId(), length, value.storageChecksumType(), value.storageChecksumValue(),
                value.contentSha256(), value.etag(), value.lifecycle(), value.lifecycleEpoch(),
                value.createdAtMillis(), value.orphanNotBeforeMillis(), value.gcAttemptId(),
                value.referenceSetSha256(), value.markedAtMillis(), value.deleteNotBeforeMillis(),
                value.deleteStartedAtMillis(), value.deletedAtMillis(), value.tombstoneFirstAbsentAtMillis(),
                value.tombstoneProofSha256(), value.stateReason(), 0);
    }

    private static ObjectReaderLeaseRecord withLeaseId(ObjectReaderLeaseRecord value, String leaseId) {
        return new ObjectReaderLeaseRecord(
                value.schemaVersion(), value.objectKeyHash(), value.processRunId(), leaseId,
                value.rootLifecycleEpoch(), value.acquiredAtMillis(), value.expiresAtMillis(),
                value.maximumReadDeadlineMillis(), value.renewalSequence(), 0);
    }

    private static ObjectProtectionRecord withOwner(ObjectProtectionRecord value, String ownerKey) {
        return new ObjectProtectionRecord(
                value.schemaVersion(), value.objectKeyHash(), value.protectionTypeId(), value.referenceId(), ownerKey,
                value.ownerMetadataVersion(), value.ownerIdentitySha256(), value.rootLifecycleEpoch(),
                value.createdAtMillis(), value.expiresAtMillis(), 0);
    }

    private static OxiaJavaPhysicalObjectMetadataStore store() {
        return new OxiaJavaPhysicalObjectMetadataStore(
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
