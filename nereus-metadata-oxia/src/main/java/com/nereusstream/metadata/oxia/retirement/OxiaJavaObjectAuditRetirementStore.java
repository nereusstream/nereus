/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectId;
import com.nereusstream.metadata.oxia.OxiaClientConfiguration;
import com.nereusstream.metadata.oxia.OxiaKeyspace;
import com.nereusstream.metadata.oxia.PartitionKey;
import com.nereusstream.metadata.oxia.SharedOxiaClientRuntime;
import com.nereusstream.metadata.oxia.records.ObjectManifestRecord;
import com.nereusstream.metadata.oxia.records.ObjectReferenceRecord;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Production exact Phase 1 audit-retirement adapter over a borrowed shared Oxia runtime. */
public final class OxiaJavaObjectAuditRetirementStore
        implements ObjectAuditRetirementStore {
    private final RetirementMetadataSupport support;

    public static OxiaJavaObjectAuditRetirementStore usingSharedRuntime(
            OxiaClientConfiguration clientConfig,
            SharedOxiaClientRuntime runtime) {
        Objects.requireNonNull(runtime, "runtime");
        return new OxiaJavaObjectAuditRetirementStore(
                runtime.retirementMetadataClient(Objects.requireNonNull(clientConfig, "clientConfig")));
    }

    OxiaJavaObjectAuditRetirementStore(RetirementMetadataClient client) {
        this.support = new RetirementMetadataSupport(client);
    }

    @Override
    public CompletableFuture<Optional<VersionedObjectManifestAudit>> getManifest(
            String cluster,
            ObjectId objectId) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        ObjectId object = Objects.requireNonNull(objectId, "objectId");
        String key = keys.objectManifestKey(object);
        return support.get(key, keys.objectPartitionKey(object)).thenApply(optional ->
                optional.map(value -> manifest(key, object, value)));
    }

    @Override
    public CompletableFuture<Optional<VersionedObjectReferencesAudit>> getReferences(
            String cluster,
            ObjectId objectId) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        ObjectId object = Objects.requireNonNull(objectId, "objectId");
        String key = keys.objectReferencesKey(object);
        return support.get(key, keys.objectPartitionKey(object)).thenApply(optional ->
                optional.map(value -> references(key, object, value)));
    }

    @Override
    public CompletableFuture<Void> deleteReferences(
            String cluster,
            ObjectId objectId,
            long expectedVersion,
            Checksum expectedDurableValueSha256) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        ObjectId object = Objects.requireNonNull(objectId, "objectId");
        String key = keys.objectReferencesKey(object);
        PartitionKey partition = keys.objectPartitionKey(object);
        return support.get(key, partition).thenCompose(optional -> {
            RetirementMetadataValue value = optional.orElseThrow(() ->
                    RetirementMetadataSupport.missing("object-reference audit"));
            references(key, object, value);
            support.requireExpected(value, expectedVersion, expectedDurableValueSha256);
            return support.delete(key, expectedVersion, partition);
        });
    }

    @Override
    public CompletableFuture<Void> deleteManifest(
            String cluster,
            ObjectId objectId,
            long expectedVersion,
            Checksum expectedDurableValueSha256) {
        OxiaKeyspace keys = new OxiaKeyspace(cluster);
        ObjectId object = Objects.requireNonNull(objectId, "objectId");
        String key = keys.objectManifestKey(object);
        PartitionKey partition = keys.objectPartitionKey(object);
        return support.get(key, partition).thenCompose(optional -> {
            RetirementMetadataValue value = optional.orElseThrow(() ->
                    RetirementMetadataSupport.missing("object-manifest audit"));
            manifest(key, object, value);
            support.requireExpected(value, expectedVersion, expectedDurableValueSha256);
            return support.delete(key, expectedVersion, partition);
        });
    }

    @Override
    public void close() {
        support.close();
    }

    private VersionedObjectManifestAudit manifest(
            String key,
            ObjectId object,
            RetirementMetadataValue value) {
        ObjectManifestRecord record = support.decode(value, ObjectManifestRecord.class);
        if (!record.objectId().equals(object.value()) || record.metadataVersion() != 0) {
            throw RetirementMetadataSupport.invariant(
                    "object-manifest audit key/value identity mismatch");
        }
        ObjectManifestRecord hydrated = new ObjectManifestRecord(
                record.objectId(),
                record.objectKey(),
                record.objectType(),
                record.state(),
                record.formatMajorVersion(),
                record.formatMinorVersion(),
                record.writerVersion(),
                record.writerId(),
                record.writerRunIdHash(),
                record.writerEpoch(),
                record.createdAtMillis(),
                record.uploadedAtMillis(),
                record.objectLength(),
                record.objectChecksumType(),
                record.objectChecksumValue(),
                record.storageChecksumType(),
                record.storageChecksumValue(),
                record.slices(),
                record.orphanExpiresAtMillis(),
                value.version());
        return new VersionedObjectManifestAudit(
                key, hydrated, value.version(), support.digest(value));
    }

    private VersionedObjectReferencesAudit references(
            String key,
            ObjectId object,
            RetirementMetadataValue value) {
        ObjectReferenceRecord record = support.decode(value, ObjectReferenceRecord.class);
        if (!record.objectId().equals(object.value()) || record.metadataVersion() != 0) {
            throw RetirementMetadataSupport.invariant(
                    "object-reference audit key/value identity mismatch");
        }
        ObjectReferenceRecord hydrated = new ObjectReferenceRecord(
                record.objectId(), record.visibleSlices(), record.updatedAtMillis(), value.version());
        return new VersionedObjectReferencesAudit(
                key, hydrated, value.version(), support.digest(value));
    }
}
