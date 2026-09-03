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

package com.nereusstream.storage.object.provider;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** Cell-scoped M5-D exact multipart upload-id abort and complete relist reconciliation. */
public final class M5MultipartCleanupSessionV1 {
    private static final byte[] INVENTORY_DOMAIN =
            "NEREUS_V2_M5_MULTIPART_INVENTORY_V1".getBytes(StandardCharsets.US_ASCII);

    public enum CleanupKindV1 {
        AUTHORITATIVELY_ABSENT,
        EXACT_OWNED_RESIDUE_REMAINS,
        DIFFERENT_OR_FOREIGN_IDENTITY,
        OUTCOME_UNKNOWN
    }

    /** A result is an adapter observation only; it is neither persisted intent nor dispatch authority. */
    public record CleanupResultV1(
            CleanupKindV1 kind,
            int completeListPasses,
            int totalListPages,
            long totalListedUploads,
            int exactAbortAttempts) {
        public CleanupResultV1 {
            Objects.requireNonNull(kind, "kind");
            if (completeListPasses <= 0
                    || totalListPages < completeListPasses
                    || totalListedUploads < 0
                    || exactAbortAttempts < 0) {
                throw new IllegalArgumentException("multipart cleanup counters are invalid");
            }
        }
    }

    private static final Comparator<ObjectProviderTransport.MultipartUploadIdentity> IDENTITY_ORDER =
            Comparator.comparing(ObjectProviderTransport.MultipartUploadIdentity::key)
                    .thenComparing(
                            ObjectProviderTransport.MultipartUploadIdentity::uploadId,
                            M5MultipartCleanupSessionV1::compareUnsigned);

    private final ObjectProviderTransport transport;
    private final CellProviderScopeId providerScopeId;
    private final String namespacePrefix;
    private final String providerIdentity;
    private final int maximumListPages;
    private final long maximumListUploads;
    private final long maximumListIdentityBytes;
    private final int maximumSingleKeyBytes;
    private final int maximumUploadIdBytes;

    public M5MultipartCleanupSessionV1(
            ObjectProviderTransport transport,
            CellProviderScopeId providerScopeId,
            String exclusiveNamespacePrefix,
            int maximumListPages,
            long maximumListUploads,
            long maximumListIdentityBytes,
            int maximumSingleKeyBytes) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.providerScopeId = Objects.requireNonNull(providerScopeId, "providerScopeId");
        if (exclusiveNamespacePrefix == null
                || exclusiveNamespacePrefix.isEmpty()
                || exclusiveNamespacePrefix.endsWith("/")
                || exclusiveNamespacePrefix.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("exclusive multipart namespace is invalid");
        }
        this.namespacePrefix = exclusiveNamespacePrefix + "/";
        transport.capabilities().requireC1();
        ObjectProviderTransport.MultipartCleanupCapabilities multipart = transport.multipartCleanupCapabilities();
        multipart.requireExactUploadIdAbortV1();
        this.providerIdentity = transport.capabilities().providerIdentity();
        if (!multipart.providerIdentity().equals(providerIdentity)) {
            throw new IllegalArgumentException("create/read and multipart Provider identities differ");
        }
        if (maximumListPages <= 0
                || maximumListUploads <= 0
                || maximumListIdentityBytes <= 0
                || maximumSingleKeyBytes <= 0
                || maximumSingleKeyBytes > 1_024) {
            throw new IllegalArgumentException("multipart reconciliation bounds must be positive and bounded");
        }
        this.maximumListPages = maximumListPages;
        this.maximumListUploads = maximumListUploads;
        this.maximumListIdentityBytes = maximumListIdentityBytes;
        this.maximumSingleKeyBytes = maximumSingleKeyBytes;
        this.maximumUploadIdBytes = multipart.maximumUploadIdBytes();
    }

    public CellProviderScopeId providerScopeId() {
        return providerScopeId;
    }

    /** Computes the exact persisted root that the caller must bind into the future M5-D intent. */
    public Sha256Digest inventoryRoot(Set<ObjectProviderTransport.MultipartUploadIdentity> exactOwnedInventory) {
        List<ObjectProviderTransport.MultipartUploadIdentity> owned = validateOwnedInventory(exactOwnedInventory);
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                writeBytes(out, INVENTORY_DOMAIN);
                writeBytes(out, providerScopeId.digest().bytes().toByteArray());
                writeBytes(out, providerIdentity.getBytes(StandardCharsets.UTF_8));
                writeBytes(out, namespacePrefix.getBytes(StandardCharsets.UTF_8));
                out.writeInt(owned.size());
                for (ObjectProviderTransport.MultipartUploadIdentity identity : owned) {
                    writeBytes(out, identity.key().getBytes(StandardCharsets.UTF_8));
                    writeBytes(out, identity.uploadId().toByteArray());
                }
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory multipart inventory encoding failed", impossible);
        }
    }

    /**
     * Aborts only exact persisted owned identities after a complete foreign-free scan and completely relists after
     * every abort result. This method cannot create intent, publish done, or decide whether dispatch is authorized.
     */
    public CleanupResultV1 cleanupOwned(
            Sha256Digest persistedInventoryRoot,
            Set<ObjectProviderTransport.MultipartUploadIdentity> exactOwnedInventory)
            throws IOException {
        Objects.requireNonNull(persistedInventoryRoot, "persistedInventoryRoot");
        List<ObjectProviderTransport.MultipartUploadIdentity> ownedList = validateOwnedInventory(exactOwnedInventory);
        Set<ObjectProviderTransport.MultipartUploadIdentity> owned = Set.copyOf(ownedList);
        if (!inventoryRoot(owned).equals(persistedInventoryRoot)) {
            throw new IllegalArgumentException("multipart inventory differs from its exact persisted root");
        }

        Counters counters = new Counters();
        ListSnapshot current = completeInventoryList(owned, counters);
        if (containsForeign(current.uploads, owned)) {
            return counters.result(CleanupKindV1.DIFFERENT_OR_FOREIGN_IDENTITY);
        }
        if (current.uploads.isEmpty()) {
            return counters.result(CleanupKindV1.AUTHORITATIVELY_ABSENT);
        }

        for (ObjectProviderTransport.MultipartUploadIdentity target :
                current.uploads.stream().sorted(IDENTITY_ORDER).toList()) {
            ObjectProviderTransport.ExactMultipartAbortResult abortResult;
            IOException abortFailure = null;
            try {
                abortResult = transport.abortMultipartUploadExact(target.key(), target.uploadId());
            } catch (IOException failure) {
                abortFailure = failure;
                abortResult = null;
            }
            counters.abortAttempts++;

            try {
                current = completeInventoryList(owned, counters);
            } catch (IOException reconciliationFailure) {
                if (abortFailure != null) {
                    reconciliationFailure.addSuppressed(abortFailure);
                }
                throw reconciliationFailure;
            }
            if (containsForeign(current.uploads, owned)) {
                return counters.result(CleanupKindV1.DIFFERENT_OR_FOREIGN_IDENTITY);
            }
            if (current.uploads.contains(target)) {
                if (abortResult == ObjectProviderTransport.ExactMultipartAbortResult.DEFINITIVE_CONFLICT
                        || abortResult == ObjectProviderTransport.ExactMultipartAbortResult.UNSUPPORTED) {
                    return counters.result(CleanupKindV1.DIFFERENT_OR_FOREIGN_IDENTITY);
                }
                return counters.result(
                        abortFailure == null
                                ? CleanupKindV1.EXACT_OWNED_RESIDUE_REMAINS
                                : CleanupKindV1.OUTCOME_UNKNOWN);
            }
        }
        return counters.result(
                current.uploads.isEmpty()
                        ? CleanupKindV1.AUTHORITATIVELY_ABSENT
                        : CleanupKindV1.EXACT_OWNED_RESIDUE_REMAINS);
    }

    private List<ObjectProviderTransport.MultipartUploadIdentity> validateOwnedInventory(
            Set<ObjectProviderTransport.MultipartUploadIdentity> exactOwnedInventory) {
        Objects.requireNonNull(exactOwnedInventory, "exactOwnedInventory");
        if (exactOwnedInventory.isEmpty()) {
            throw new IllegalArgumentException("exact owned multipart inventory must be non-empty");
        }
        List<ObjectProviderTransport.MultipartUploadIdentity> owned = new ArrayList<>(exactOwnedInventory.size());
        for (ObjectProviderTransport.MultipartUploadIdentity identity : exactOwnedInventory) {
            validateIdentity(identity);
            owned.add(identity);
        }
        owned.sort(IDENTITY_ORDER);
        return List.copyOf(owned);
    }

    private ListSnapshot completeInventoryList(
            Set<ObjectProviderTransport.MultipartUploadIdentity> owned, Counters counters) throws IOException {
        Set<String> exactKeys = new TreeSet<>();
        owned.forEach(identity -> exactKeys.add(identity.key()));
        Set<ObjectProviderTransport.MultipartUploadIdentity> uploads = new HashSet<>();
        int pages = 0;
        long identityBytes = 0;
        for (String exactKey : exactKeys) {
            ListSnapshot keySnapshot = completeListForExactKey(exactKey);
            pages = Math.addExact(pages, keySnapshot.pages);
            if (pages > maximumListPages) {
                throw new IOException("multipart reconciliation exceeded its aggregate LIST page cap");
            }
            for (ObjectProviderTransport.MultipartUploadIdentity identity : keySnapshot.uploads) {
                if (!uploads.add(identity)) {
                    throw new IOException("multipart reconciliation LIST returned a duplicate identity");
                }
                identityBytes = Math.addExact(
                        identityBytes,
                        Math.addExact(
                                identity.key().getBytes(StandardCharsets.UTF_8).length,
                                identity.uploadId().length()));
                if (uploads.size() > maximumListUploads || identityBytes > maximumListIdentityBytes) {
                    throw new IOException("multipart reconciliation exceeded its aggregate upload/byte cap");
                }
            }
        }
        counters.listPasses++;
        counters.listPages = Math.addExact(counters.listPages, pages);
        counters.listedUploads = Math.addExact(counters.listedUploads, uploads.size());
        return new ListSnapshot(Set.copyOf(uploads), pages);
    }

    private ListSnapshot completeListForExactKey(String exactKey) throws IOException {
        Optional<CanonicalBytes> continuation = Optional.empty();
        Set<CanonicalBytes> seenTokens = new HashSet<>();
        Set<ObjectProviderTransport.MultipartUploadIdentity> uploads = new HashSet<>();
        int pages = 0;
        long identityBytes = 0;
        do {
            if (++pages > maximumListPages) {
                throw new IOException("multipart reconciliation exceeded its LIST page cap");
            }
            ObjectProviderTransport.MultipartListPage page = transport.listMultipartUploads(
                    exactKey,
                    continuation,
                    transport.multipartCleanupCapabilities().maximumListPageUploads());
            for (ObjectProviderTransport.MultipartUploadIdentity identity : page.uploads()) {
                validateIdentity(identity);
                if (!identity.key().equals(exactKey)) {
                    throw new IOException("multipart exact-key LIST returned a different key");
                }
                if (!uploads.add(identity)) {
                    throw new IOException("multipart reconciliation LIST returned a duplicate identity");
                }
                identityBytes = Math.addExact(
                        identityBytes,
                        Math.addExact(
                                identity.key().getBytes(StandardCharsets.UTF_8).length,
                                identity.uploadId().length()));
                if (uploads.size() > maximumListUploads || identityBytes > maximumListIdentityBytes) {
                    throw new IOException("multipart reconciliation exceeded its upload/byte cap");
                }
            }
            Optional<CanonicalBytes> next = page.nextContinuationToken();
            if (next.isPresent()) {
                CanonicalBytes token = next.orElseThrow();
                if (token.length() > transport.multipartCleanupCapabilities().maximumContinuationTokenBytes()
                        || !seenTokens.add(token)
                        || continuation.filter(token::equals).isPresent()) {
                    throw new IOException("multipart reconciliation LIST returned an invalid or repeated token");
                }
            }
            continuation = next;
        } while (continuation.isPresent());
        return new ListSnapshot(Set.copyOf(uploads), pages);
    }

    private void validateIdentity(ObjectProviderTransport.MultipartUploadIdentity identity) {
        Objects.requireNonNull(identity, "multipart identity");
        byte[] key = identity.key().getBytes(StandardCharsets.UTF_8);
        if (!identity.key().startsWith(namespacePrefix)
                || key.length > maximumSingleKeyBytes
                || !identity.key().equals(new String(key, StandardCharsets.UTF_8))
                || identity.uploadId().length() > maximumUploadIdBytes) {
            throw new IllegalArgumentException("multipart identity is outside the exact Cell namespace or hard caps");
        }
    }

    private static boolean containsForeign(
            Set<ObjectProviderTransport.MultipartUploadIdentity> observed,
            Set<ObjectProviderTransport.MultipartUploadIdentity> owned) {
        return observed.stream().anyMatch(identity -> !owned.contains(identity));
    }

    private static int compareUnsigned(CanonicalBytes left, CanonicalBytes right) {
        return java.util.Arrays.compareUnsigned(left.toByteArray(), right.toByteArray());
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private record ListSnapshot(Set<ObjectProviderTransport.MultipartUploadIdentity> uploads, int pages) {}

    private static final class Counters {
        private int listPasses;
        private int listPages;
        private long listedUploads;
        private int abortAttempts;

        private CleanupResultV1 result(CleanupKindV1 kind) {
            return new CleanupResultV1(kind, listPasses, listPages, listedUploads, abortAttempts);
        }
    }
}
