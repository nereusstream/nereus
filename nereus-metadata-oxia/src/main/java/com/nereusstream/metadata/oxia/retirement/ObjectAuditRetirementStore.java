/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia.retirement;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectId;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Exact read/delete-only surface for Phase 1 object audit metadata. */
public interface ObjectAuditRetirementStore extends AutoCloseable {
    CompletableFuture<Optional<VersionedObjectManifestAudit>> getManifest(
            String cluster, ObjectId objectId);

    CompletableFuture<Optional<VersionedObjectReferencesAudit>> getReferences(
            String cluster, ObjectId objectId);

    CompletableFuture<Void> deleteReferences(
            String cluster,
            ObjectId objectId,
            long expectedVersion,
            Checksum expectedDurableValueSha256);

    CompletableFuture<Void> deleteManifest(
            String cluster,
            ObjectId objectId,
            long expectedVersion,
            Checksum expectedDurableValueSha256);

    @Override
    void close();
}
