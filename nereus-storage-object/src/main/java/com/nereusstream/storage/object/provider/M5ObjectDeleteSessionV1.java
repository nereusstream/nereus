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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Cell-scoped M5-D full-GET, exact-version delete, and complete LIST/full-GET reconciliation. */
public final class M5ObjectDeleteSessionV1 {
    public enum ReconciliationKindV1 {
        AUTHORITATIVELY_ABSENT,
        EXACT_OLD_VERSION_REMAINS,
        DIFFERENT_VERSION_OR_BODY,
        OUTCOME_UNKNOWN
    }

    /** Exact full-body identity and immutable version observed by the same Provider response. */
    public record ExactDeleteReadV1(ObjectIdentity identity, CanonicalBytes immutableVersionToken) {
        public ExactDeleteReadV1 {
            Objects.requireNonNull(identity, "identity");
            immutableVersionToken = copyRequired(immutableVersionToken, "immutableVersionToken");
        }
    }

    /** Result of a complete bounded LIST followed by an exact full GET. */
    public record ReconciliationV1(
            ReconciliationKindV1 kind,
            int listPages,
            long listedKeys,
            boolean exactKeyListed,
            Optional<CanonicalBytes> observedVersionToken) {
        public ReconciliationV1 {
            Objects.requireNonNull(kind, "kind");
            observedVersionToken = Objects.requireNonNull(observedVersionToken, "observedVersionToken")
                    .map(value -> copyRequired(value, "observedVersionToken"));
            if (listPages <= 0 || listedKeys < 0) {
                throw new IllegalArgumentException("delete reconciliation LIST counters are invalid");
            }
            if ((kind == ReconciliationKindV1.AUTHORITATIVELY_ABSENT
                            && (exactKeyListed || observedVersionToken.isPresent()))
                    || (kind == ReconciliationKindV1.EXACT_OLD_VERSION_REMAINS && observedVersionToken.isEmpty())) {
                throw new IllegalArgumentException("delete reconciliation kind and facts disagree");
            }
        }
    }

    private final ObjectProviderTransport transport;
    private final CellProviderScopeId providerScopeId;
    private final String namespacePrefix;
    private final long maximumObjectBytes;
    private final int maximumListPages;
    private final long maximumListKeys;
    private final long maximumListKeyBytes;
    private final int maximumSingleKeyBytes;

    public M5ObjectDeleteSessionV1(
            ObjectProviderTransport transport,
            CellProviderScopeId providerScopeId,
            String exclusiveNamespacePrefix,
            long maximumObjectBytes,
            int maximumListPages,
            long maximumListKeys,
            long maximumListKeyBytes,
            int maximumSingleKeyBytes) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.providerScopeId = Objects.requireNonNull(providerScopeId, "providerScopeId");
        if (exclusiveNamespacePrefix == null
                || exclusiveNamespacePrefix.isEmpty()
                || exclusiveNamespacePrefix.endsWith("/")
                || exclusiveNamespacePrefix.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("exclusive delete namespace is invalid");
        }
        this.namespacePrefix = exclusiveNamespacePrefix + "/";
        transport.capabilities().requireC1();
        transport.deleteCapabilities().requireVersionMatchDeleteV1();
        if (!transport
                .deleteCapabilities()
                .providerIdentity()
                .equals(transport.capabilities().providerIdentity())) {
            throw new IllegalArgumentException("create/read and delete Provider identities differ");
        }
        if (maximumObjectBytes <= 0
                || maximumObjectBytes > transport.capabilities().maximumObjectBytes()) {
            throw new IllegalArgumentException("delete Object cap exceeds the admitted Provider capability");
        }
        if (maximumListPages <= 0
                || maximumListKeys <= 0
                || maximumListKeyBytes <= 0
                || maximumSingleKeyBytes <= 0
                || maximumSingleKeyBytes > 1_024) {
            throw new IllegalArgumentException("delete reconciliation bounds must be positive and bounded");
        }
        this.maximumObjectBytes = maximumObjectBytes;
        this.maximumListPages = maximumListPages;
        this.maximumListKeys = maximumListKeys;
        this.maximumListKeyBytes = maximumListKeyBytes;
        this.maximumSingleKeyBytes = maximumSingleKeyBytes;
    }

    public CellProviderScopeId providerScopeId() {
        return providerScopeId;
    }

    public ExactDeleteReadV1 readExactForDelete(ObjectIdentity identity) throws IOException {
        requireOwned(identity);
        ReadObservation observation = readCurrent(identity);
        if (observation.kind != ReadKind.EXACT || observation.versionToken.isEmpty()) {
            throw new IOException("M5-D full GET did not return the exact body and immutable version token");
        }
        return new ExactDeleteReadV1(identity, observation.versionToken.orElseThrow());
    }

    public ObjectProviderTransport.ConditionalDeleteResult deleteExactVersion(
            ObjectIdentity identity, CanonicalBytes exactVersionToken) throws IOException {
        requireOwned(identity);
        CanonicalBytes token = copyRequired(exactVersionToken, "exactVersionToken");
        if (token.length() > transport.deleteCapabilities().maximumVersionTokenBytes()) {
            throw new IllegalArgumentException("delete version token exceeds the admitted hard cap");
        }
        return transport.deleteExactVersion(identity.key(), token);
    }

    public ReconciliationV1 reconcile(ObjectIdentity identity, String leafPrefix, CanonicalBytes oldVersionToken)
            throws IOException {
        requireOwned(identity);
        requireOwnedPrefix(leafPrefix);
        CanonicalBytes expectedOld = copyRequired(oldVersionToken, "oldVersionToken");
        ListObservation list = completeList(leafPrefix, identity.key());
        ReadObservation read = readCurrent(identity);
        if (read.kind == ReadKind.NOT_FOUND) {
            return new ReconciliationV1(
                    list.exactKeyListed
                            ? ReconciliationKindV1.OUTCOME_UNKNOWN
                            : ReconciliationKindV1.AUTHORITATIVELY_ABSENT,
                    list.pages,
                    list.keys,
                    list.exactKeyListed,
                    Optional.empty());
        }
        if (read.kind == ReadKind.UNKNOWN) {
            return new ReconciliationV1(
                    ReconciliationKindV1.OUTCOME_UNKNOWN, list.pages, list.keys, list.exactKeyListed, Optional.empty());
        }
        if (read.kind == ReadKind.DIFFERENT
                || read.versionToken.isEmpty()
                || !read.versionToken.orElseThrow().equals(expectedOld)) {
            return new ReconciliationV1(
                    ReconciliationKindV1.DIFFERENT_VERSION_OR_BODY,
                    list.pages,
                    list.keys,
                    list.exactKeyListed,
                    read.versionToken);
        }
        return new ReconciliationV1(
                ReconciliationKindV1.EXACT_OLD_VERSION_REMAINS,
                list.pages,
                list.keys,
                list.exactKeyListed,
                read.versionToken);
    }

    private ReadObservation readCurrent(ObjectIdentity identity) throws IOException {
        try (ObjectProviderTransport.StreamingObject response = transport.get(identity.key(), Optional.empty())) {
            if (response.bodyLength() <= 0
                    || response.bodyLength() > maximumObjectBytes
                    || response.inclusiveStart() != 0
                    || response.exclusiveEnd() != response.bodyLength()
                    || response.bodyLength() > Integer.MAX_VALUE) {
                return new ReadObservation(ReadKind.DIFFERENT, Optional.empty());
            }
            CanonicalBytes body = readExact(response.body(), Math.toIntExact(response.bodyLength()));
            Optional<CanonicalBytes> token = response.immutableVersionToken()
                    .map(value -> copyRequired(value, "Provider immutable version token"));
            if (response.bodyLength() != identity.bodyLength()
                    || !Sha256Digest.hash(body).equals(identity.bodySha256())) {
                return new ReadObservation(ReadKind.DIFFERENT, token);
            }
            return new ReadObservation(ReadKind.EXACT, token);
        } catch (IOException failure) {
            ObjectProviderTransport.FailureKind kind = transport.classifyFailure(failure);
            if (kind == ObjectProviderTransport.FailureKind.NOT_FOUND) {
                return new ReadObservation(ReadKind.NOT_FOUND, Optional.empty());
            }
            if (kind == ObjectProviderTransport.FailureKind.FATAL) {
                throw failure;
            }
            return new ReadObservation(ReadKind.UNKNOWN, Optional.empty());
        }
    }

    private ListObservation completeList(String prefix, String exactKey) throws IOException {
        Optional<CanonicalBytes> continuation = Optional.empty();
        Set<CanonicalBytes> seenTokens = new HashSet<>();
        int pages = 0;
        long keys = 0;
        long keyBytes = 0;
        boolean listed = false;
        String previousKey = null;
        do {
            if (++pages > maximumListPages) {
                throw new IOException("delete reconciliation exceeded its LIST page cap");
            }
            ObjectProviderTransport.ListPage page = transport.list(
                    prefix, continuation, transport.capabilities().maximumListPageKeys());
            for (ObjectProviderTransport.ListedObject object : page.objects()) {
                byte[] encoded = object.key().getBytes(StandardCharsets.UTF_8);
                if (!object.key().startsWith(prefix)
                        || encoded.length > maximumSingleKeyBytes
                        || !object.key().equals(new String(encoded, StandardCharsets.UTF_8))
                        || (previousKey != null && object.key().compareTo(previousKey) <= 0)) {
                    throw new IOException("delete reconciliation LIST returned a foreign or non-canonical key");
                }
                previousKey = object.key();
                keys = Math.addExact(keys, 1);
                keyBytes = Math.addExact(keyBytes, encoded.length);
                if (keys > maximumListKeys || keyBytes > maximumListKeyBytes) {
                    throw new IOException("delete reconciliation exceeded its LIST key/byte cap");
                }
                listed |= object.key().equals(exactKey);
            }
            Optional<CanonicalBytes> next = page.nextContinuationToken();
            if (next.isPresent() && !seenTokens.add(next.orElseThrow())) {
                throw new IOException("delete reconciliation LIST repeated a continuation token");
            }
            continuation = next;
        } while (continuation.isPresent());
        return new ListObservation(pages, keys, listed);
    }

    private void requireOwned(ObjectIdentity identity) {
        Objects.requireNonNull(identity, "identity");
        if (!identity.key().startsWith(namespacePrefix)
                || identity.bodyLength() <= 0
                || identity.bodyLength() > maximumObjectBytes) {
            throw new IllegalArgumentException("delete target is outside the exact Cell namespace or Object cap");
        }
    }

    private void requireOwnedPrefix(String prefix) {
        if (prefix == null || !prefix.startsWith(namespacePrefix) || !prefix.endsWith("/")) {
            throw new IllegalArgumentException("delete reconciliation prefix is outside the exact Cell namespace");
        }
    }

    private static CanonicalBytes readExact(InputStream input, int length) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream(length);
        byte[] buffer = new byte[Math.min(64 * 1024, length)];
        int remaining = length;
        while (remaining > 0) {
            int read = input.read(buffer, 0, Math.min(buffer.length, remaining));
            if (read < 0) {
                throw new IOException("Provider full GET ended before the declared length");
            }
            if (read == 0) {
                int one = input.read();
                if (one < 0) {
                    throw new IOException("Provider full GET ended before the declared length");
                }
                output.write(one);
                remaining--;
            } else {
                output.write(buffer, 0, read);
                remaining -= read;
            }
        }
        if (input.read() != -1) {
            throw new IOException("Provider full GET exceeded the declared length");
        }
        return CanonicalBytes.copyOf(output.toByteArray());
    }

    private static CanonicalBytes copyRequired(CanonicalBytes value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isEmpty()) {
            throw new IllegalArgumentException(label + " must be non-empty");
        }
        return CanonicalBytes.copyOf(value.toByteArray());
    }

    private enum ReadKind {
        EXACT,
        DIFFERENT,
        NOT_FOUND,
        UNKNOWN
    }

    private record ReadObservation(ReadKind kind, Optional<CanonicalBytes> versionToken) {}

    private record ListObservation(int pages, long keys, boolean exactKeyListed) {}
}
