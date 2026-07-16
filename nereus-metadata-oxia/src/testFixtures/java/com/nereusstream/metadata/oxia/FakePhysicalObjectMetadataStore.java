/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.codec.F4MetadataCodecs;
import com.nereusstream.metadata.oxia.records.GcRetirementManifestRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementProtectionRecord;
import com.nereusstream.metadata.oxia.records.GcRetirementRemovalRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectProtectionType;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectLifecycle;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Deterministic in-memory physical store used by unchanged M1 store/core contracts. */
public class FakePhysicalObjectMetadataStore implements PhysicalObjectMetadataStore {
    private final Map<String, VersionedPhysicalObjectRoot> roots = new HashMap<>();
    private final Map<String, VersionedReaderLease> leases = new HashMap<>();
    private final Map<String, VersionedObjectProtection> protections = new HashMap<>();
    private final Map<String, VersionedGcRetirementManifest> retirementManifests = new HashMap<>();
    private final Map<String, VersionedGcRetirementProtection> retirementProtections = new HashMap<>();
    private final Map<String, VersionedGcRetirementRemoval> retirementRemovals = new HashMap<>();
    private long nextVersion = 1;
    private boolean closed;

    @Override
    public synchronized CompletableFuture<Optional<VersionedPhysicalObjectRoot>> getRoot(
            String cluster, ObjectKeyHash object) {
        ensureOpen();
        return completed(Optional.ofNullable(roots.get(new F4Keyspace(cluster).physicalRootKey(object))));
    }

    @Override
    public synchronized CompletableFuture<VersionedPhysicalObjectRoot> createRoot(
            String cluster, PhysicalObjectRootRecord root) {
        ensureOpen();
        F4Keyspace keys = new F4Keyspace(cluster);
        ObjectKeyHash object = new ObjectKeyHash(root.objectKeyHash());
        String key = keys.physicalRootKey(object);
        VersionedPhysicalObjectRoot existing = roots.get(key);
        if (existing != null) {
            if (!PhysicalObjectRootTransitions.sameImmutableIdentity(root, existing.value())) {
                return failed(new F4MetadataConditionFailedException("physical root immutable identity conflict"));
            }
            return completed(existing);
        }
        long version = nextVersion++;
        PhysicalObjectRootRecord value = root.withMetadataVersion(version);
        VersionedPhysicalObjectRoot created = new VersionedPhysicalObjectRoot(
                key, value, version, durable(value, PhysicalObjectRootRecord.class));
        roots.put(key, created);
        return completed(created);
    }

    @Override
    public synchronized CompletableFuture<VersionedPhysicalObjectRoot> compareAndSetRoot(
            String cluster, PhysicalObjectRootRecord root, long expectedVersion) {
        ensureOpen();
        F4Keyspace keys = new F4Keyspace(cluster);
        ObjectKeyHash object = new ObjectKeyHash(root.objectKeyHash());
        String key = keys.physicalRootKey(object);
        VersionedPhysicalObjectRoot current = roots.get(key);
        if (current == null || current.metadataVersion() != expectedVersion) {
            return failed(new F4MetadataConditionFailedException("physical root version mismatch"));
        }
        try {
            PhysicalObjectRootTransitions.requireValidReplacement(current.value(), root);
        } catch (RuntimeException failure) {
            return failed(failure);
        }
        long version = nextVersion++;
        PhysicalObjectRootRecord value = root.withMetadataVersion(version);
        VersionedPhysicalObjectRoot updated = new VersionedPhysicalObjectRoot(
                key, value, version, durable(value, PhysicalObjectRootRecord.class));
        roots.put(key, updated);
        return completed(updated);
    }

    @Override
    public synchronized CompletableFuture<Void> deleteRoot(
            String cluster, ObjectKeyHash object, long expectedVersion, Checksum expectedRootSha256) {
        ensureOpen();
        String key = new F4Keyspace(cluster).physicalRootKey(object);
        VersionedPhysicalObjectRoot current = roots.get(key);
        if (current == null
                || current.metadataVersion() != expectedVersion
                || !current.durableValueSha256().equals(expectedRootSha256)
                || current.value().lifecycle() != PhysicalObjectLifecycle.DELETED
                || current.value().tombstoneFirstAbsentAtMillis() == 0) {
            return failed(new F4MetadataConditionFailedException("physical root delete proof mismatch"));
        }
        roots.remove(key);
        return completed(null);
    }

    @Override
    public synchronized CompletableFuture<PhysicalObjectRootScanPage> scanRoots(
            String cluster, int shard, Optional<F4ScanToken> continuation, int limit) {
        ensureOpen();
        F4MetadataStoreSupport.requirePageLimit(limit);
        F4Keyspace keys = new F4Keyspace(cluster);
        String base = keys.physicalRootShardPrefix(shard);
        String prefix = F4MetadataStoreSupport.prefixStart(base);
        String scope = sha256("physical-root\0" + shard);
        List<VersionedPhysicalObjectRoot> values = page(
                roots.values(),
                VersionedPhysicalObjectRoot::key,
                prefix,
                continuation,
                cluster,
                F4ScanKind.PHYSICAL_ROOT,
                scope,
                limit);
        return completed(new PhysicalObjectRootScanPage(
                values,
                continuation(values, VersionedPhysicalObjectRoot::key, cluster,
                        F4ScanKind.PHYSICAL_ROOT, scope, prefix, limit)));
    }

    @Override
    public synchronized CompletableFuture<VersionedReaderLease> createOrCompareReaderLease(
            String cluster, ObjectReaderLeaseRecord lease) {
        ensureOpen();
        F4Keyspace keys = new F4Keyspace(cluster);
        ObjectKeyHash object = new ObjectKeyHash(lease.objectKeyHash());
        String key = keys.readerLeaseKey(object, lease.processRunId());
        VersionedReaderLease existing = leases.get(key);
        if (existing != null) {
            if (!existing.value().withMetadataVersion(0).equals(lease.withMetadataVersion(0))) {
                return failed(new F4MetadataConditionFailedException("reader lease identity conflict"));
            }
            return completed(existing);
        }
        long version = nextVersion++;
        ObjectReaderLeaseRecord value = lease.withMetadataVersion(version);
        VersionedReaderLease created = new VersionedReaderLease(
                key, value, version, durable(value, ObjectReaderLeaseRecord.class));
        leases.put(key, created);
        return completed(created);
    }

    @Override
    public synchronized CompletableFuture<VersionedReaderLease> compareAndSetReaderLease(
            String cluster, ObjectReaderLeaseRecord lease, long expectedVersion) {
        ensureOpen();
        F4Keyspace keys = new F4Keyspace(cluster);
        ObjectKeyHash object = new ObjectKeyHash(lease.objectKeyHash());
        String key = keys.readerLeaseKey(object, lease.processRunId());
        VersionedReaderLease current = leases.get(key);
        if (current == null || current.metadataVersion() != expectedVersion) {
            return failed(new F4MetadataConditionFailedException("reader lease version mismatch"));
        }
        long version = nextVersion++;
        ObjectReaderLeaseRecord value = lease.withMetadataVersion(version);
        VersionedReaderLease updated = new VersionedReaderLease(
                key, value, version, durable(value, ObjectReaderLeaseRecord.class));
        leases.put(key, updated);
        return completed(updated);
    }

    @Override
    public synchronized CompletableFuture<Void> deleteReaderLease(
            String cluster, ObjectKeyHash object, String processRunId, long expectedVersion) {
        ensureOpen();
        String key = new F4Keyspace(cluster).readerLeaseKey(object, processRunId);
        VersionedReaderLease current = leases.get(key);
        if (current == null || current.metadataVersion() != expectedVersion) {
            return failed(new F4MetadataConditionFailedException("reader lease delete version mismatch"));
        }
        leases.remove(key);
        return completed(null);
    }

    @Override
    public synchronized CompletableFuture<ReaderLeaseScanPage> scanReaderLeases(
            String cluster, ObjectKeyHash object, Optional<F4ScanToken> continuation, int limit) {
        ensureOpen();
        F4MetadataStoreSupport.requirePageLimit(limit);
        F4Keyspace keys = new F4Keyspace(cluster);
        String prefix = F4MetadataStoreSupport.prefixStart(keys.readerPrefix(object));
        String scope = sha256(
                F4ScanKind.READER_LEASE.name() + "\0" + object.value() + "\0" + keys.readerPrefix(object));
        List<VersionedReaderLease> values = page(
                leases.values(), VersionedReaderLease::key, prefix, continuation,
                cluster, F4ScanKind.READER_LEASE, scope, limit);
        return completed(new ReaderLeaseScanPage(
                values,
                continuation(values, VersionedReaderLease::key, cluster,
                        F4ScanKind.READER_LEASE, scope, prefix, limit)));
    }

    @Override
    public synchronized CompletableFuture<VersionedObjectProtection> createProtection(
            String cluster, ObjectProtectionRecord protection) {
        ensureOpen();
        F4Keyspace keys = new F4Keyspace(cluster);
        ObjectKeyHash object = new ObjectKeyHash(protection.objectKeyHash());
        ObjectProtectionType type = ObjectProtectionType.fromWireId(protection.protectionTypeId());
        String key = keys.protectionKey(object, type, protection.referenceId());
        VersionedObjectProtection existing = protections.get(key);
        if (existing != null) {
            if (!existing.value().withMetadataVersion(0).equals(protection.withMetadataVersion(0))) {
                return failed(new F4MetadataConditionFailedException("object protection identity conflict"));
            }
            return completed(existing);
        }
        long version = nextVersion++;
        ObjectProtectionRecord value = protection.withMetadataVersion(version);
        VersionedObjectProtection created = new VersionedObjectProtection(
                key, value, version, durable(value, ObjectProtectionRecord.class));
        protections.put(key, created);
        return completed(created);
    }

    @Override
    public synchronized CompletableFuture<VersionedObjectProtection> compareAndSetProtection(
            String cluster, ObjectProtectionRecord protection, long expectedVersion) {
        ensureOpen();
        F4Keyspace keys = new F4Keyspace(cluster);
        ObjectKeyHash object = new ObjectKeyHash(protection.objectKeyHash());
        ObjectProtectionType type = ObjectProtectionType.fromWireId(protection.protectionTypeId());
        String key = keys.protectionKey(object, type, protection.referenceId());
        VersionedObjectProtection current = protections.get(key);
        if (current == null || current.metadataVersion() != expectedVersion) {
            return failed(new F4MetadataConditionFailedException("object protection version mismatch"));
        }
        long version = nextVersion++;
        ObjectProtectionRecord value = protection.withMetadataVersion(version);
        VersionedObjectProtection updated = new VersionedObjectProtection(
                key, value, version, durable(value, ObjectProtectionRecord.class));
        protections.put(key, updated);
        return completed(updated);
    }

    @Override
    public synchronized CompletableFuture<Void> deleteProtection(
            String cluster, ObjectProtectionIdentity protection, long expectedVersion) {
        ensureOpen();
        F4Keyspace keys = new F4Keyspace(cluster);
        String key = keys.protectionKey(protection.object(), protection.type(), protection.referenceId());
        VersionedObjectProtection current = protections.get(key);
        if (current == null || current.metadataVersion() != expectedVersion) {
            return failed(new F4MetadataConditionFailedException("object protection delete version mismatch"));
        }
        protections.remove(key);
        return completed(null);
    }

    @Override
    public synchronized CompletableFuture<ObjectProtectionScanPage> scanProtections(
            String cluster, ObjectKeyHash object, Optional<F4ScanToken> continuation, int limit) {
        ensureOpen();
        F4MetadataStoreSupport.requirePageLimit(limit);
        F4Keyspace keys = new F4Keyspace(cluster);
        String prefix = F4MetadataStoreSupport.prefixStart(keys.protectionPrefix(object));
        String scope = sha256(F4ScanKind.OBJECT_PROTECTION.name()
                + "\0"
                + object.value()
                + "\0"
                + keys.protectionPrefix(object));
        List<VersionedObjectProtection> values = page(
                protections.values(), VersionedObjectProtection::key, prefix, continuation,
                cluster, F4ScanKind.OBJECT_PROTECTION, scope, limit);
        return completed(new ObjectProtectionScanPage(
                values,
                continuation(values, VersionedObjectProtection::key, cluster,
                        F4ScanKind.OBJECT_PROTECTION, scope, prefix, limit)));
    }

    @Override
    public synchronized CompletableFuture<Optional<VersionedGcRetirementManifest>> getRetirementManifest(
            String cluster, ObjectKeyHash object, String gcAttemptId) {
        ensureOpen();
        String key = new F4Keyspace(cluster).gcRetirementManifestKey(object, gcAttemptId);
        return completed(Optional.ofNullable(retirementManifests.get(key)));
    }

    @Override
    public synchronized CompletableFuture<VersionedGcRetirementManifest> createRetirementManifest(
            String cluster, GcRetirementManifestRecord manifest) {
        ensureOpen();
        F4Keyspace keys = new F4Keyspace(cluster);
        ObjectKeyHash object = new ObjectKeyHash(manifest.objectKeyHash());
        String key = keys.gcRetirementManifestKey(object, manifest.gcAttemptId());
        VersionedGcRetirementManifest existing = retirementManifests.get(key);
        if (existing != null) {
            if (!existing.value().withMetadataVersion(0).equals(manifest)) {
                return failed(F4MetadataStoreSupport.invariant(
                        "GC retirement manifest identity conflict"));
            }
            return completed(existing);
        }
        long version = nextVersion++;
        GcRetirementManifestRecord value = manifest.withMetadataVersion(version);
        VersionedGcRetirementManifest created = new VersionedGcRetirementManifest(
                key, value, version, durable(value, GcRetirementManifestRecord.class));
        retirementManifests.put(key, created);
        return completed(created);
    }

    @Override
    public synchronized CompletableFuture<VersionedGcRetirementProtection> createRetirementProtection(
            String cluster, GcRetirementProtectionRecord protection) {
        ensureOpen();
        F4Keyspace keys = new F4Keyspace(cluster);
        ObjectKeyHash object = new ObjectKeyHash(protection.objectKeyHash());
        String key = keys.gcRetirementProtectionKey(
                object, protection.gcAttemptId(), protection.protectionKey());
        VersionedGcRetirementProtection existing = retirementProtections.get(key);
        if (existing != null) {
            if (!existing.value().withMetadataVersion(0).equals(protection)) {
                return failed(F4MetadataStoreSupport.invariant(
                        "GC retirement protection identity conflict"));
            }
            return completed(existing);
        }
        long version = nextVersion++;
        GcRetirementProtectionRecord value = protection.withMetadataVersion(version);
        VersionedGcRetirementProtection created = new VersionedGcRetirementProtection(
                key, value, version, durable(value, GcRetirementProtectionRecord.class));
        retirementProtections.put(key, created);
        return completed(created);
    }

    @Override
    public synchronized CompletableFuture<VersionedGcRetirementRemoval> createRetirementRemoval(
            String cluster, GcRetirementRemovalRecord removal) {
        ensureOpen();
        F4Keyspace keys = new F4Keyspace(cluster);
        ObjectKeyHash object = new ObjectKeyHash(removal.objectKeyHash());
        String key = keys.gcRetirementRemovalKey(
                object, removal.gcAttemptId(), removal.removalKey());
        VersionedGcRetirementRemoval existing = retirementRemovals.get(key);
        if (existing != null) {
            if (!existing.value().withMetadataVersion(0).equals(removal)) {
                return failed(F4MetadataStoreSupport.invariant(
                        "GC retirement removal identity conflict"));
            }
            return completed(existing);
        }
        long version = nextVersion++;
        GcRetirementRemovalRecord value = removal.withMetadataVersion(version);
        VersionedGcRetirementRemoval created = new VersionedGcRetirementRemoval(
                key, value, version, durable(value, GcRetirementRemovalRecord.class));
        retirementRemovals.put(key, created);
        return completed(created);
    }

    @Override
    public synchronized CompletableFuture<GcRetirementProtectionScanPage> scanRetirementProtections(
            String cluster,
            ObjectKeyHash object,
            String gcAttemptId,
            Optional<F4ScanToken> continuation,
            int limit) {
        ensureOpen();
        F4MetadataStoreSupport.requirePageLimit(limit);
        F4Keyspace keys = new F4Keyspace(cluster);
        String base = keys.gcRetirementProtectionPrefix(object, gcAttemptId);
        String prefix = F4MetadataStoreSupport.prefixStart(base);
        String scope = sha256(F4ScanKind.GC_RETIREMENT_PROTECTION.name()
                + "\0"
                + object.value()
                + "\0"
                + base);
        List<VersionedGcRetirementProtection> values = page(
                retirementProtections.values(),
                VersionedGcRetirementProtection::key,
                prefix,
                continuation,
                cluster,
                F4ScanKind.GC_RETIREMENT_PROTECTION,
                scope,
                limit);
        return completed(new GcRetirementProtectionScanPage(
                values,
                continuation(
                        values,
                        VersionedGcRetirementProtection::key,
                        cluster,
                        F4ScanKind.GC_RETIREMENT_PROTECTION,
                        scope,
                        prefix,
                        limit)));
    }

    @Override
    public synchronized CompletableFuture<GcRetirementRemovalScanPage> scanRetirementRemovals(
            String cluster,
            ObjectKeyHash object,
            String gcAttemptId,
            Optional<F4ScanToken> continuation,
            int limit) {
        ensureOpen();
        F4MetadataStoreSupport.requirePageLimit(limit);
        F4Keyspace keys = new F4Keyspace(cluster);
        String base = keys.gcRetirementRemovalPrefix(object, gcAttemptId);
        String prefix = F4MetadataStoreSupport.prefixStart(base);
        String scope = sha256(F4ScanKind.GC_RETIREMENT_REMOVAL.name()
                + "\0"
                + object.value()
                + "\0"
                + base);
        List<VersionedGcRetirementRemoval> values = page(
                retirementRemovals.values(),
                VersionedGcRetirementRemoval::key,
                prefix,
                continuation,
                cluster,
                F4ScanKind.GC_RETIREMENT_REMOVAL,
                scope,
                limit);
        return completed(new GcRetirementRemovalScanPage(
                values,
                continuation(
                        values,
                        VersionedGcRetirementRemoval::key,
                        cluster,
                        F4ScanKind.GC_RETIREMENT_REMOVAL,
                        scope,
                        prefix,
                        limit)));
    }

    public synchronized Optional<VersionedReaderLease> readerLease(
            String cluster, ObjectKeyHash object, String processRunId) {
        return Optional.ofNullable(leases.get(new F4Keyspace(cluster).readerLeaseKey(object, processRunId)));
    }

    public synchronized Optional<VersionedObjectProtection> protection(
            String cluster, ObjectProtectionIdentity identity) {
        F4Keyspace keys = new F4Keyspace(cluster);
        return Optional.ofNullable(protections.get(
                keys.protectionKey(identity.object(), identity.type(), identity.referenceId())));
    }

    @Override
    public synchronized void close() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("fake physical object metadata store is closed");
        }
    }

    private static <T> List<T> page(
            java.util.Collection<T> source,
            java.util.function.Function<T, String> key,
            String prefix,
            Optional<F4ScanToken> continuation,
            String cluster,
            F4ScanKind kind,
            String scope,
            int limit) {
        String after = validateToken(continuation, cluster, kind, scope, prefix)
                .map(F4ScanToken::exclusiveLastKey)
                .orElse("");
        return source.stream()
                .filter(value -> key.apply(value).startsWith(prefix))
                .filter(value -> after.isEmpty() || key.apply(value).compareTo(after) > 0)
                .sorted(Comparator.comparing(key))
                .limit(limit)
                .toList();
    }

    private static <T> Optional<F4ScanToken> continuation(
            List<T> values,
            java.util.function.Function<T, String> key,
            String cluster,
            F4ScanKind kind,
            String scope,
            String prefix,
            int limit) {
        return values.size() == limit
                ? Optional.of(new F4ScanToken(
                        cluster, kind, scope, prefix, key.apply(values.get(values.size() - 1))))
                : Optional.empty();
    }

    private static Optional<F4ScanToken> validateToken(
            Optional<F4ScanToken> supplied,
            String cluster,
            F4ScanKind kind,
            String scope,
            String prefix) {
        Objects.requireNonNull(supplied, "continuation");
        supplied.ifPresent(token -> {
            if (!cluster.equals(token.cluster())
                    || kind != token.kind()
                    || !scope.equals(token.scopeIdentitySha256())
                    || !prefix.equals(token.scanPrefix())) {
                throw new IllegalArgumentException("continuation token does not match scan scope");
            }
        });
        return supplied;
    }

    private static <T> Checksum durable(T value, Class<T> type) {
        try {
            byte[] encoded = F4MetadataCodecs.encodeEnvelope(value, type);
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(encoded);
            return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(digest));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static <T> CompletableFuture<T> completed(T value) {
        return CompletableFuture.completedFuture(value);
    }

    private static <T> CompletableFuture<T> failed(Throwable failure) {
        return CompletableFuture.failedFuture(failure);
    }
}
