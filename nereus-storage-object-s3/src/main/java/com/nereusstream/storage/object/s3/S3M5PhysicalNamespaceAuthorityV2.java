/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nereusstream.storage.object.s3;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.CanonicalUtf8;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.api.lifecycle.MetadataNamespaceIdentityV2;
import com.nereusstream.storage.api.lifecycle.NativePhysicalNamespaceAuthorityV2;
import com.nereusstream.storage.api.lifecycle.PhysicalNamespaceAuthorityBindingV2;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2;
import com.nereusstream.storage.object.gc.BoundPhysicalDeleteAuthorityRouteV2;
import com.nereusstream.storage.object.provider.ObjectIdentity;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * One native S3 marker version assigns one physical bucket incarnation to one native Oxia namespace.
 * Its immutable version ID is part of the physical identity. Rewrite, delete marker and incomplete
 * history fail closed; exact deletion and recreation produce a different physical identity.
 */
public final class S3M5PhysicalNamespaceAuthorityV2 implements NativePhysicalNamespaceAuthorityV2 {
    public static final String MARKER_KEY = "v2/m5-physical-object-namespace-v2";
    private static final int MAGIC = 0x4d354f4e; // M5ON
    private static final int MARKER_BYTES = 152;

    private final S3Client client;
    private final String bucket;
    private final Marker marker;
    private final PhysicalResourceIdV2.Namespace namespace;

    private S3M5PhysicalNamespaceAuthorityV2(S3Client client, String bucket, Marker marker) {
        this.client = client;
        this.bucket = bucket;
        this.marker = marker;
        this.namespace = namespace(bucket, marker);
    }

    /** Explicit provisioning only; a lost PUT response is reconciled by a complete native reread. */
    public static S3M5PhysicalNamespaceAuthorityV2 provision(
            S3Client client, String bucket, MetadataNamespaceIdentityV2 metadataNamespace) throws IOException {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(metadataNamespace, "metadataNamespace");
        String exactBucket = requireBucket(bucket);
        requireVersioning(client, exactBucket);
        if (!versions(client, exactBucket).isEmpty()) {
            throw new IllegalStateException("existing S3 physical namespace requires explicit reconnect");
        }
        var inventory = client.listObjectVersions(ListObjectVersionsRequest.builder()
                .bucket(exactBucket)
                .maxKeys(1)
                .build());
        if (Boolean.TRUE.equals(inventory.isTruncated())
                || !inventory.versions().isEmpty()
                || !inventory.deleteMarkers().isEmpty()) {
            throw new IllegalStateException("S3 namespace provisioning requires an empty version history");
        }
        var uploads = client.listMultipartUploads(ListMultipartUploadsRequest.builder()
                .bucket(exactBucket)
                .maxUploads(1)
                .build());
        if (Boolean.TRUE.equals(uploads.isTruncated()) || !uploads.uploads().isEmpty()) {
            throw new IllegalStateException("S3 namespace provisioning requires no active multipart uploads");
        }
        UUID candidate = UUID.randomUUID();
        RuntimeException putFailure = null;
        try {
            client.putObject(
                    PutObjectRequest.builder()
                            .bucket(exactBucket)
                            .key(MARKER_KEY)
                            .ifNoneMatch("*")
                            .build(),
                    RequestBody.fromBytes(encode(candidate, metadataNamespace).toByteArray()));
        } catch (RuntimeException uncertain) {
            // A competing provisioner or a lost response is resolved below, without adopting another marker.
            putFailure = uncertain;
        }
        Marker observed;
        try {
            observed = readMarker(client, exactBucket);
        } catch (IOException | RuntimeException failure) {
            if (putFailure != null) {
                failure.addSuppressed(putFailure);
            }
            throw failure;
        }
        if (!observed.instance().equals(candidate)
                || !observed.metadataNamespace().equals(metadataNamespace)) {
            var failure =
                    new IllegalStateException("S3 physical namespace provisioning did not create the exact marker");
            if (putFailure != null) {
                failure.addSuppressed(putFailure);
            }
            throw failure;
        }
        return new S3M5PhysicalNamespaceAuthorityV2(client, exactBucket, observed);
    }

    /** Reconnect requires the previously recorded physical identity; a fresh marker cannot alias it. */
    public static S3M5PhysicalNamespaceAuthorityV2 connect(
            S3Client client, String bucket, PhysicalResourceIdV2.Namespace expected) throws IOException {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(expected, "expected");
        String exactBucket = requireBucket(bucket);
        Marker observed = readMarker(client, exactBucket);
        if (!namespace(exactBucket, observed).equals(expected)) {
            throw new IllegalStateException("S3 physical namespace incarnation differs");
        }
        return new S3M5PhysicalNamespaceAuthorityV2(client, exactBucket, observed);
    }

    /** This adapter owns the actual Provider client and bucket used for native assignment reads. */
    public S3C1ObjectProviderTransport openVersionMatchDeleteTransport(int maximumListPageKeys) throws IOException {
        requireCurrent();
        return S3C1ObjectProviderTransport.admitVersionMatchDeleteV1(
                client, bucket, namespace.serviceIdentity().value(), false, maximumListPageKeys);
    }

    public S3C1ObjectProviderTransport openMultipartCleanupTransport(int maximumListPageKeys) throws IOException {
        requireCurrent();
        return S3C1ObjectProviderTransport.admitMultipartCleanupV1(
                client, bucket, namespace.serviceIdentity().value(), false, maximumListPageKeys);
    }

    /** One exact upload ID remains a separate permanent resource from its Object key/version. */
    public S3M5MultipartDeleteIdentityReaderV2 openMultipartDeleteIdentityReader(
            BoundPhysicalDeleteAuthorityRouteV2 route,
            CellProviderScopeId scope,
            String exclusiveNamespacePrefix,
            String exactKey,
            CanonicalBytes uploadId,
            int maximumListPages,
            long maximumListUploads,
            long maximumListIdentityBytes,
            int maximumSingleKeyBytes,
            int maximumListPageKeys)
            throws IOException {
        var transport = openMultipartCleanupTransport(maximumListPageKeys);
        try {
            return new S3M5MultipartDeleteIdentityReaderV2(
                    this,
                    route,
                    transport,
                    scope,
                    exclusiveNamespacePrefix,
                    exactKey,
                    uploadId,
                    maximumListPages,
                    maximumListUploads,
                    maximumListIdentityBytes,
                    maximumSingleKeyBytes);
        } catch (RuntimeException failure) {
            transport.close();
            throw failure;
        }
    }

    /** The native identity reader and its Provider session cannot be assembled from different backends. */
    public S3M5ObjectDeleteIdentityReaderV2 openDeleteIdentityReader(
            BoundPhysicalDeleteAuthorityRouteV2 route,
            CellProviderScopeId scope,
            String exclusiveNamespacePrefix,
            String leafPrefix,
            ObjectIdentity identity,
            CanonicalBytes versionToken,
            long maximumObjectBytes,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes,
            int maximumListPageKeys)
            throws IOException {
        return openDeleteIdentityReader(
                route,
                scope,
                exclusiveNamespacePrefix,
                leafPrefix,
                identity,
                versionToken,
                maximumObjectBytes,
                maximumListPages,
                maximumListKeys,
                maximumListKeyBytes,
                maximumSingleKeyBytes,
                maximumListPageKeys,
                Runnable::run);
    }

    public S3M5ObjectDeleteIdentityReaderV2 openDeleteIdentityReader(
            BoundPhysicalDeleteAuthorityRouteV2 route,
            CellProviderScopeId scope,
            String exclusiveNamespacePrefix,
            String leafPrefix,
            ObjectIdentity identity,
            CanonicalBytes versionToken,
            long maximumObjectBytes,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes,
            int maximumListPageKeys,
            java.util.concurrent.Executor ioExecutor)
            throws IOException {
        var transport = openVersionMatchDeleteTransport(maximumListPageKeys);
        try {
            return new S3M5ObjectDeleteIdentityReaderV2(
                    this,
                    route,
                    transport,
                    scope,
                    exclusiveNamespacePrefix,
                    leafPrefix,
                    identity,
                    versionToken,
                    maximumObjectBytes,
                    maximumListPages,
                    maximumListKeys,
                    maximumListKeyBytes,
                    maximumSingleKeyBytes,
                    ioExecutor);
        } catch (RuntimeException failure) {
            transport.close();
            throw failure;
        }
    }

    @Override
    public PhysicalResourceIdV2.Namespace namespace() {
        return namespace;
    }

    @Override
    public CompletionStage<Optional<PhysicalNamespaceAuthorityBindingV2>> readBinding() {
        try {
            return CompletableFuture.completedFuture(Optional.of(new PhysicalNamespaceAuthorityBindingV2(
                    namespace, requireCurrent().metadataNamespace())));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    /** Provisioning installed the permanent binding; this call only verifies its exact native identity. */
    @Override
    public CompletionStage<PhysicalNamespaceAuthorityBindingV2> bind(MetadataNamespaceIdentityV2 metadataNamespace) {
        Objects.requireNonNull(metadataNamespace, "metadataNamespace");
        try {
            Marker current = requireCurrent();
            if (!current.metadataNamespace().equals(metadataNamespace)) {
                throw new IllegalStateException("S3 physical namespace belongs to another Oxia namespace");
            }
            return CompletableFuture.completedFuture(
                    new PhysicalNamespaceAuthorityBindingV2(namespace, metadataNamespace));
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private Marker requireCurrent() throws IOException {
        Marker current = readMarker(client, bucket);
        if (!current.equals(marker)) {
            throw new IllegalStateException("S3 physical namespace marker changed");
        }
        return current;
    }

    private static Marker readMarker(S3Client client, String bucket) throws IOException {
        requireVersioning(client, bucket);
        List<String> versions = versions(client, bucket);
        if (versions.size() != 1) {
            throw new IllegalStateException("S3 namespace marker is absent or has multiple versions");
        }
        String version = versions.get(0);
        Marker marker;
        try (var input = client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(MARKER_KEY)
                .versionId(version)
                .build())) {
            byte[] body = input.readNBytes(MARKER_BYTES + 1);
            if (body.length != MARKER_BYTES || !version.equals(input.response().versionId())) {
                throw new IllegalStateException("S3 namespace marker exact version or length differs");
            }
            marker = decode(CanonicalBytes.copyOf(body), version);
        }
        try (var current = client.getObject(
                GetObjectRequest.builder().bucket(bucket).key(MARKER_KEY).build())) {
            if (!version.equals(current.response().versionId())) {
                throw new IllegalStateException("S3 namespace marker current version differs from history");
            }
        }
        return marker;
    }

    private static List<String> versions(S3Client client, String bucket) {
        var response = client.listObjectVersions(ListObjectVersionsRequest.builder()
                .bucket(bucket)
                .prefix(MARKER_KEY)
                .maxKeys(2)
                .build());
        if (Boolean.TRUE.equals(response.isTruncated())
                || response.deleteMarkers().stream().anyMatch(value -> MARKER_KEY.equals(value.key()))) {
            throw new IllegalStateException("S3 namespace marker history is truncated or deleted");
        }
        var exact = response.versions().stream()
                .filter(value -> MARKER_KEY.equals(value.key()))
                .toList();
        if (exact.size() > 1
                || !exact.isEmpty() && !Boolean.TRUE.equals(exact.get(0).isLatest())) {
            throw new IllegalStateException("S3 namespace marker was rewritten");
        }
        return exact.stream().map(value -> value.versionId()).toList();
    }

    private static void requireVersioning(S3Client client, String bucket) {
        if (client.getBucketVersioning(GetBucketVersioningRequest.builder()
                                .bucket(bucket)
                                .build())
                        .status()
                != BucketVersioningStatus.ENABLED) {
            throw new IllegalStateException("S3 physical namespace requires enabled bucket versioning");
        }
    }

    private static PhysicalResourceIdV2.Namespace namespace(String bucket, Marker marker) {
        var versionDigest =
                Sha256Digest.hash(CanonicalBytes.copyOf(marker.version().getBytes(StandardCharsets.UTF_8)));
        return new PhysicalResourceIdV2.Namespace(
                PhysicalResourceIdV2.ProviderKind.OBJECT_PROVIDER,
                CanonicalUtf8.fromString("s3-namespace:" + marker.instance() + ":" + versionDigest.toHex()),
                CanonicalUtf8.fromString(bucket));
    }

    private static CanonicalBytes encode(UUID instance, MetadataNamespaceIdentityV2 metadataNamespace) {
        var out = ByteBuffer.allocate(MARKER_BYTES)
                .putInt(MAGIC)
                .putInt(2)
                .putLong(instance.getMostSignificantBits())
                .putLong(instance.getLeastSignificantBits())
                .put(metadataNamespace.encode().toByteArray());
        out.put(Sha256Digest.hash(CanonicalBytes.copyOf(Arrays.copyOf(out.array(), out.position())))
                .bytes()
                .toByteArray());
        return CanonicalBytes.copyOf(out.array());
    }

    private static Marker decode(CanonicalBytes bytes, String version) {
        if (bytes.length() != MARKER_BYTES || version == null || version.isBlank()) {
            throw new IllegalArgumentException("S3 namespace marker length or version differs");
        }
        var in = ByteBuffer.wrap(bytes.toByteArray());
        if (in.getInt() != MAGIC || in.getInt() != 2) {
            throw new IllegalArgumentException("S3 namespace marker wire differs");
        }
        UUID instance = new UUID(in.getLong(), in.getLong());
        byte[] identity = new byte[MetadataNamespaceIdentityV2.IDENTITY_BYTES];
        in.get(identity);
        MetadataNamespaceIdentityV2 metadataNamespace =
                MetadataNamespaceIdentityV2.decode(CanonicalBytes.copyOf(identity));
        if (!encode(instance, metadataNamespace).equals(bytes)) {
            throw new IllegalArgumentException("S3 namespace marker checksum or canonical bytes differ");
        }
        return new Marker(instance, metadataNamespace, version);
    }

    private static String requireBucket(String bucket) {
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("S3 bucket is blank");
        }
        return bucket;
    }

    private record Marker(UUID instance, MetadataNamespaceIdentityV2 metadataNamespace, String version) {}
}
