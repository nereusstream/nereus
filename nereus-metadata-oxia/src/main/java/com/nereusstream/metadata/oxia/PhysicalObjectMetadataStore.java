/* Licensed under the Apache License, Version 2.0 */
package com.nereusstream.metadata.oxia;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ObjectKeyHash;
import com.nereusstream.metadata.oxia.records.ObjectProtectionRecord;
import com.nereusstream.metadata.oxia.records.ObjectReaderLeaseRecord;
import com.nereusstream.metadata.oxia.records.PhysicalObjectRootRecord;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Focused root/reader/protection metadata surface for physical deletion safety. */
public interface PhysicalObjectMetadataStore extends AutoCloseable {
    CompletableFuture<Optional<VersionedPhysicalObjectRoot>> getRoot(String cluster, ObjectKeyHash object);

    CompletableFuture<VersionedPhysicalObjectRoot> createRoot(String cluster, PhysicalObjectRootRecord root);

    CompletableFuture<VersionedPhysicalObjectRoot> compareAndSetRoot(
            String cluster, PhysicalObjectRootRecord root, long expectedVersion);

    CompletableFuture<Void> deleteRoot(
            String cluster, ObjectKeyHash object, long expectedVersion, Checksum expectedRootSha256);

    CompletableFuture<PhysicalObjectRootScanPage> scanRoots(
            String cluster, int shard, Optional<F4ScanToken> continuation, int limit);

    CompletableFuture<VersionedReaderLease> createOrCompareReaderLease(
            String cluster, ObjectReaderLeaseRecord lease);

    CompletableFuture<VersionedReaderLease> compareAndSetReaderLease(
            String cluster, ObjectReaderLeaseRecord lease, long expectedVersion);

    CompletableFuture<Void> deleteReaderLease(
            String cluster, ObjectKeyHash object, String processRunId, long expectedVersion);

    CompletableFuture<ReaderLeaseScanPage> scanReaderLeases(
            String cluster, ObjectKeyHash object, Optional<F4ScanToken> continuation, int limit);

    CompletableFuture<VersionedObjectProtection> createProtection(
            String cluster, ObjectProtectionRecord protection);

    CompletableFuture<VersionedObjectProtection> compareAndSetProtection(
            String cluster, ObjectProtectionRecord protection, long expectedVersion);

    CompletableFuture<Void> deleteProtection(
            String cluster, ObjectProtectionIdentity protection, long expectedVersion);

    CompletableFuture<ObjectProtectionScanPage> scanProtections(
            String cluster, ObjectKeyHash object, Optional<F4ScanToken> continuation, int limit);

    @Override
    void close();
}
