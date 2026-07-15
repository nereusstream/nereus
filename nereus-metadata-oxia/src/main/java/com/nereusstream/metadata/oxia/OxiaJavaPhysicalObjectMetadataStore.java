/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** Production Phase 4 physical-root/lease/protection adapter over the shared Oxia runtime. */
public final class OxiaJavaPhysicalObjectMetadataStore implements PhysicalObjectMetadataStore {
    private final F4MetadataStoreSupport support;

    public static OxiaJavaPhysicalObjectMetadataStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime,
            Clock clock) {
        Objects.requireNonNull(clientConfig, "clientConfig");
        Objects.requireNonNull(runtime, "runtime");
        runtime.requireCompatible(clientConfig);
        return new OxiaJavaPhysicalObjectMetadataStore(runtime.client(), clock);
    }

    OxiaJavaPhysicalObjectMetadataStore(PartitionedOxiaClient client, Clock clock) {
        this.support = new F4MetadataStoreSupport(client, clock);
    }

    @Override
    public CompletableFuture<Optional<VersionedPhysicalObjectRoot>> getRoot(
            String cluster, ObjectKeyHash object) {
        F4Keyspace keys = new F4Keyspace(cluster);
        ObjectKeyHash exact = Objects.requireNonNull(object, "object");
        return support.get(
                        keys.physicalRootKey(exact),
                        keys.physicalObjectPartitionKey(exact),
                        PhysicalObjectRootRecord.class)
                .thenApply(value -> value.map(item -> root(keys, item)));
    }

    @Override
    public CompletableFuture<VersionedPhysicalObjectRoot> createRoot(
            String cluster, PhysicalObjectRootRecord root) {
        PhysicalObjectRootRecord value = Objects.requireNonNull(root, "root");
        ObjectKeyHash object = new ObjectKeyHash(value.objectKeyHash());
        F4Keyspace keys = new F4Keyspace(cluster);
        CompletableFuture<VersionedPhysicalObjectRoot> create = support.create(
                        keys.physicalRootKey(object),
                        keys.physicalObjectPartitionKey(object),
                        value,
                        PhysicalObjectRootRecord.class)
                .thenApply(item -> root(keys, item));
        return create.handle((created, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(created);
            }
            if (!F4MetadataStoreSupport.isConditionFailure(failure)) {
                return F4MetadataStoreSupport.<VersionedPhysicalObjectRoot>failed(
                        F4MetadataStoreSupport.unwrap(failure));
            }
            return getRoot(cluster, object).thenApply(existing -> {
                VersionedPhysicalObjectRoot result = existing.orElseThrow(
                        () -> F4MetadataStoreSupport.invariant("physical root disappeared after create conflict"));
                if (!sameImmutableIdentity(value, result.value())) {
                    throw F4MetadataStoreSupport.invariant("physical root immutable identity conflict");
                }
                return result;
            });
        }).thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<VersionedPhysicalObjectRoot> compareAndSetRoot(
            String cluster, PhysicalObjectRootRecord root, long expectedVersion) {
        PhysicalObjectRootRecord value = Objects.requireNonNull(root, "root");
        ObjectKeyHash object = new ObjectKeyHash(value.objectKeyHash());
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.compareAndSet(
                        keys.physicalRootKey(object),
                        keys.physicalObjectPartitionKey(object),
                        value,
                        PhysicalObjectRootRecord.class,
                        expectedVersion)
                .thenApply(item -> root(keys, item));
    }

    @Override
    public CompletableFuture<Void> deleteRoot(
            String cluster,
            ObjectKeyHash object,
            long expectedVersion,
            Checksum expectedRootSha256) {
        Objects.requireNonNull(expectedRootSha256, "expectedRootSha256");
        return getRoot(cluster, object).thenCompose(optional -> {
            VersionedPhysicalObjectRoot current = optional.orElseThrow(
                    () -> new F4MetadataConditionFailedException("physical root is absent"));
            if (current.metadataVersion() != expectedVersion
                    || !current.durableValueSha256().equals(expectedRootSha256)
                    || current.value().lifecycle() != PhysicalObjectLifecycle.DELETED
                    || current.value().tombstoneFirstAbsentAtMillis() == 0) {
                return F4MetadataStoreSupport.failed(new F4MetadataConditionFailedException(
                        "physical root delete proof does not match current DELETED tombstone"));
            }
            F4Keyspace keys = new F4Keyspace(cluster);
            return support.delete(
                    keys.physicalRootKey(object), keys.physicalObjectPartitionKey(object), expectedVersion);
        });
    }

    @Override
    public CompletableFuture<PhysicalObjectRootScanPage> scanRoots(
            String cluster, int shard, Optional<F4ScanToken> continuation, int limit) {
        F4MetadataStoreSupport.requirePageLimit(limit);
        F4Keyspace keys = new F4Keyspace(cluster);
        String base = keys.physicalRootShardPrefix(shard);
        String prefix = F4MetadataStoreSupport.prefixStart(base);
        String scope = support.scopeSha256("physical-root\0" + shard);
        F4ScanToken token = support.validateToken(
                continuation, keys.cluster(), F4ScanKind.PHYSICAL_ROOT, scope, prefix);
        String from = token == null ? prefix : token.resumeFromInclusive();
        String to = F4MetadataStoreSupport.prefixEnd(base);
        ObjectKeyHash partitionHash = hashForShard(shard);
        return support.client().rangeScan(
                        from, to, limit, keys.physicalObjectPartitionKey(partitionHash))
                .thenApply(stored -> {
                    List<VersionedPhysicalObjectRoot> values = stored.stream()
                            .map(item -> root(keys, support.decode(item, PhysicalObjectRootRecord.class)))
                            .toList();
                    Optional<F4ScanToken> next = stored.size() == limit
                            ? Optional.of(new F4ScanToken(
                                    keys.cluster(), F4ScanKind.PHYSICAL_ROOT, scope, prefix,
                                    stored.get(stored.size() - 1).key()))
                            : Optional.empty();
                    return new PhysicalObjectRootScanPage(values, next);
                });
    }

    @Override
    public CompletableFuture<VersionedReaderLease> createOrCompareReaderLease(
            String cluster, ObjectReaderLeaseRecord lease) {
        ObjectReaderLeaseRecord value = Objects.requireNonNull(lease, "lease");
        ObjectKeyHash object = new ObjectKeyHash(value.objectKeyHash());
        F4Keyspace keys = new F4Keyspace(cluster);
        String key = keys.readerLeaseKey(object, value.processRunId());
        CompletableFuture<VersionedReaderLease> create = support.create(
                        key, keys.physicalObjectPartitionKey(object), value, ObjectReaderLeaseRecord.class)
                .thenApply(item -> lease(keys, item));
        return create.handle((created, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(created);
            }
            if (!F4MetadataStoreSupport.isConditionFailure(failure)) {
                return F4MetadataStoreSupport.<VersionedReaderLease>failed(
                        F4MetadataStoreSupport.unwrap(failure));
            }
            return readLease(keys, object, value.processRunId()).thenApply(existing -> {
                VersionedReaderLease result = existing.orElseThrow(
                        () -> F4MetadataStoreSupport.invariant("reader lease disappeared after create conflict"));
                if (!result.value().withMetadataVersion(0).equals(value)) {
                    throw F4MetadataStoreSupport.invariant("reader lease identity conflict");
                }
                return result;
            });
        }).thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<VersionedReaderLease> compareAndSetReaderLease(
            String cluster, ObjectReaderLeaseRecord lease, long expectedVersion) {
        ObjectReaderLeaseRecord value = Objects.requireNonNull(lease, "lease");
        ObjectKeyHash object = new ObjectKeyHash(value.objectKeyHash());
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.compareAndSet(
                        keys.readerLeaseKey(object, value.processRunId()),
                        keys.physicalObjectPartitionKey(object),
                        value,
                        ObjectReaderLeaseRecord.class,
                        expectedVersion)
                .thenApply(item -> lease(keys, item));
    }

    @Override
    public CompletableFuture<Void> deleteReaderLease(
            String cluster, ObjectKeyHash object, String processRunId, long expectedVersion) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.delete(
                keys.readerLeaseKey(object, processRunId),
                keys.physicalObjectPartitionKey(object),
                expectedVersion);
    }

    @Override
    public CompletableFuture<ReaderLeaseScanPage> scanReaderLeases(
            String cluster,
            ObjectKeyHash object,
            Optional<F4ScanToken> continuation,
            int limit) {
        F4Keyspace keys = new F4Keyspace(cluster);
        String base = keys.readerPrefix(object);
        return scanObjectPrefix(
                keys,
                object,
                F4ScanKind.READER_LEASE,
                base,
                continuation,
                limit,
                ObjectReaderLeaseRecord.class,
                item -> lease(keys, item),
                ReaderLeaseScanPage::new);
    }

    @Override
    public CompletableFuture<VersionedObjectProtection> createProtection(
            String cluster, ObjectProtectionRecord protection) {
        ObjectProtectionRecord value = Objects.requireNonNull(protection, "protection");
        ObjectKeyHash object = new ObjectKeyHash(value.objectKeyHash());
        ObjectProtectionType type = ObjectProtectionType.fromWireId(value.protectionTypeId());
        F4Keyspace keys = new F4Keyspace(cluster);
        String key = keys.protectionKey(object, type, value.referenceId());
        CompletableFuture<VersionedObjectProtection> create = support.create(
                        key, keys.physicalObjectPartitionKey(object), value, ObjectProtectionRecord.class)
                .thenApply(item -> protection(keys, item));
        return create.handle((created, failure) -> {
            if (failure == null) {
                return CompletableFuture.completedFuture(created);
            }
            if (!F4MetadataStoreSupport.isConditionFailure(failure)) {
                return F4MetadataStoreSupport.<VersionedObjectProtection>failed(
                        F4MetadataStoreSupport.unwrap(failure));
            }
            return readProtection(keys, object, type, value.referenceId()).thenApply(existing -> {
                VersionedObjectProtection result = existing.orElseThrow(
                        () -> F4MetadataStoreSupport.invariant("protection disappeared after create conflict"));
                if (!result.value().withMetadataVersion(0).equals(value)) {
                    throw F4MetadataStoreSupport.invariant("protection identity conflict");
                }
                return result;
            });
        }).thenCompose(Function.identity());
    }

    @Override
    public CompletableFuture<VersionedObjectProtection> compareAndSetProtection(
            String cluster, ObjectProtectionRecord protection, long expectedVersion) {
        ObjectProtectionRecord value = Objects.requireNonNull(protection, "protection");
        ObjectKeyHash object = new ObjectKeyHash(value.objectKeyHash());
        ObjectProtectionType type = ObjectProtectionType.fromWireId(value.protectionTypeId());
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.compareAndSet(
                        keys.protectionKey(object, type, value.referenceId()),
                        keys.physicalObjectPartitionKey(object),
                        value,
                        ObjectProtectionRecord.class,
                        expectedVersion)
                .thenApply(item -> protection(keys, item));
    }

    @Override
    public CompletableFuture<Void> deleteProtection(
            String cluster, ObjectProtectionIdentity protection, long expectedVersion) {
        ObjectProtectionIdentity value = Objects.requireNonNull(protection, "protection");
        F4Keyspace keys = new F4Keyspace(cluster);
        return support.delete(
                keys.protectionKey(value.object(), value.type(), value.referenceId()),
                keys.physicalObjectPartitionKey(value.object()),
                expectedVersion);
    }

    @Override
    public CompletableFuture<ObjectProtectionScanPage> scanProtections(
            String cluster,
            ObjectKeyHash object,
            Optional<F4ScanToken> continuation,
            int limit) {
        F4Keyspace keys = new F4Keyspace(cluster);
        String base = keys.protectionPrefix(object);
        return scanObjectPrefix(
                keys,
                object,
                F4ScanKind.OBJECT_PROTECTION,
                base,
                continuation,
                limit,
                ObjectProtectionRecord.class,
                item -> protection(keys, item),
                ObjectProtectionScanPage::new);
    }

    @Override
    public void close() {
        support.close();
    }

    private CompletableFuture<Optional<VersionedReaderLease>> readLease(
            F4Keyspace keys, ObjectKeyHash object, String processRunId) {
        return support.get(
                        keys.readerLeaseKey(object, processRunId),
                        keys.physicalObjectPartitionKey(object),
                        ObjectReaderLeaseRecord.class)
                .thenApply(value -> value.map(item -> lease(keys, item)));
    }

    private CompletableFuture<Optional<VersionedObjectProtection>> readProtection(
            F4Keyspace keys,
            ObjectKeyHash object,
            ObjectProtectionType type,
            String referenceId) {
        return support.get(
                        keys.protectionKey(object, type, referenceId),
                        keys.physicalObjectPartitionKey(object),
                        ObjectProtectionRecord.class)
                .thenApply(value -> value.map(item -> protection(keys, item)));
    }

    private <R, V, P> CompletableFuture<P> scanObjectPrefix(
            F4Keyspace keys,
            ObjectKeyHash object,
            F4ScanKind kind,
            String base,
            Optional<F4ScanToken> continuation,
            int limit,
            Class<R> recordType,
            Function<F4MetadataStoreSupport.Decoded<R>, V> wrapper,
            java.util.function.BiFunction<List<V>, Optional<F4ScanToken>, P> pageFactory) {
        F4MetadataStoreSupport.requirePageLimit(limit);
        String prefix = F4MetadataStoreSupport.prefixStart(base);
        String scope = support.scopeSha256(kind.name() + "\0" + object.value());
        F4ScanToken token = support.validateToken(continuation, keys.cluster(), kind, scope, prefix);
        String from = token == null ? prefix : token.resumeFromInclusive();
        String to = F4MetadataStoreSupport.prefixEnd(base);
        return support.client().rangeScan(
                        from, to, limit, keys.physicalObjectPartitionKey(object))
                .thenApply(stored -> {
                    List<V> values = stored.stream()
                            .map(item -> wrapper.apply(support.decode(item, recordType)))
                            .toList();
                    Optional<F4ScanToken> next = stored.size() == limit
                            ? Optional.of(new F4ScanToken(
                                    keys.cluster(), kind, scope, prefix,
                                    stored.get(stored.size() - 1).key()))
                            : Optional.empty();
                    return pageFactory.apply(values, next);
                });
    }

    private static VersionedPhysicalObjectRoot root(
            F4Keyspace keys, F4MetadataStoreSupport.Decoded<PhysicalObjectRootRecord> item) {
        PhysicalObjectRootRecord value = item.value();
        ObjectKeyHash object = new ObjectKeyHash(value.objectKeyHash());
        if (!item.key().equals(keys.physicalRootKey(object))) {
            throw F4MetadataStoreSupport.invariant("physical root key/value identity mismatch");
        }
        return new VersionedPhysicalObjectRoot(item.key(), value, item.version(), item.durableSha256());
    }

    private static VersionedReaderLease lease(
            F4Keyspace keys, F4MetadataStoreSupport.Decoded<ObjectReaderLeaseRecord> item) {
        ObjectReaderLeaseRecord value = item.value();
        ObjectKeyHash object = new ObjectKeyHash(value.objectKeyHash());
        if (!item.key().equals(keys.readerLeaseKey(object, value.processRunId()))) {
            throw F4MetadataStoreSupport.invariant("reader lease key/value identity mismatch");
        }
        return new VersionedReaderLease(item.key(), value, item.version(), item.durableSha256());
    }

    private static VersionedObjectProtection protection(
            F4Keyspace keys, F4MetadataStoreSupport.Decoded<ObjectProtectionRecord> item) {
        ObjectProtectionRecord value = item.value();
        ObjectKeyHash object = new ObjectKeyHash(value.objectKeyHash());
        ObjectProtectionType type = ObjectProtectionType.fromWireId(value.protectionTypeId());
        if (!item.key().equals(keys.protectionKey(object, type, value.referenceId()))) {
            throw F4MetadataStoreSupport.invariant("protection key/value identity mismatch");
        }
        return new VersionedObjectProtection(item.key(), value, item.version(), item.durableSha256());
    }

    private static boolean sameImmutableIdentity(
            PhysicalObjectRootRecord expected, PhysicalObjectRootRecord actual) {
        return expected.objectKeyHash().equals(actual.objectKeyHash())
                && expected.objectKey().equals(actual.objectKey())
                && expected.objectId().equals(actual.objectId())
                && expected.objectKindId() == actual.objectKindId()
                && expected.objectLength() == actual.objectLength()
                && expected.storageChecksumType().equals(actual.storageChecksumType())
                && expected.storageChecksumValue().equals(actual.storageChecksumValue())
                && expected.contentSha256().equals(actual.contentSha256())
                && expected.etag().equals(actual.etag());
    }

    private static ObjectKeyHash hashForShard(int shard) {
        if (shard < 0 || shard > 255) {
            throw new IllegalArgumentException("physical object shard must be in [0, 255]");
        }
        String alphabet = "abcdefghijklmnopqrstuvwxyz234567";
        int first = shard >>> 3;
        int secondHigh = (shard & 7) << 2;
        return new ObjectKeyHash(
                "" + alphabet.charAt(first) + alphabet.charAt(secondHigh) + "a".repeat(50));
    }
}
