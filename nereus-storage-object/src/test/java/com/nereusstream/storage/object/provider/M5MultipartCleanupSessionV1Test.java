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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.storage.api.bookkeeper.CellProviderScopeId;
import com.nereusstream.storage.object.provider.M5MultipartCleanupSessionV1.CleanupKindV1;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class M5MultipartCleanupSessionV1Test {
    @Test
    void defaultUnsupportedTransportFailsBeforeMultipartIo() {
        BaseTransport unsupported = new BaseTransport();

        assertThatThrownBy(() -> session(unsupported))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("EXACT_UPLOAD_ID_ABORT_V1");
        assertThat(unsupported.listCalls).isZero();
    }

    @Test
    void exactPersistedOwnedInventoryIsAbortedAndCompletelyRelisted() throws Exception {
        FakeMultipartTransport transport = new FakeMultipartTransport();
        var first = upload("cell-a/attempt/data", "upload-1");
        var alreadyAbsent = upload("cell-a/attempt/root", "upload-2");
        transport.uploads.add(first);
        M5MultipartCleanupSessionV1 session = session(transport);
        Set<ObjectProviderTransport.MultipartUploadIdentity> owned = Set.of(first, alreadyAbsent);

        var result = session.cleanupOwned(session.inventoryRoot(owned), owned);

        assertThat(result.kind()).isEqualTo(CleanupKindV1.AUTHORITATIVELY_ABSENT);
        assertThat(result.completeListPasses()).isEqualTo(2);
        assertThat(result.exactAbortAttempts()).isEqualTo(1);
        assertThat(transport.uploads).isEmpty();
    }

    @Test
    void foreignUploadVetoesAllAbortBeforeMutation() throws Exception {
        FakeMultipartTransport transport = new FakeMultipartTransport();
        var ownedUpload = upload("cell-a/attempt/data", "owned");
        var foreignUpload = upload("cell-a/attempt/data", "foreign");
        transport.uploads.addAll(Set.of(ownedUpload, foreignUpload));
        M5MultipartCleanupSessionV1 session = session(transport);
        Set<ObjectProviderTransport.MultipartUploadIdentity> owned = Set.of(ownedUpload);

        var result = session.cleanupOwned(session.inventoryRoot(owned), owned);

        assertThat(result.kind()).isEqualTo(CleanupKindV1.DIFFERENT_OR_FOREIGN_IDENTITY);
        assertThat(result.exactAbortAttempts()).isZero();
        assertThat(transport.abortCalls).isZero();
        assertThat(transport.uploads).containsExactlyInAnyOrder(ownedUpload, foreignUpload);
    }

    @Test
    void lostAbortResponseAdvancesOnlyAfterCompleteEmptyRelist() throws Exception {
        FakeMultipartTransport transport = new FakeMultipartTransport();
        transport.unknownAfterAbort = true;
        var target = upload("cell-a/attempt/data", "response-loss");
        transport.uploads.add(target);
        M5MultipartCleanupSessionV1 session = session(transport);
        Set<ObjectProviderTransport.MultipartUploadIdentity> owned = Set.of(target);

        var result = session.cleanupOwned(session.inventoryRoot(owned), owned);

        assertThat(result.kind()).isEqualTo(CleanupKindV1.AUTHORITATIVELY_ABSENT);
        assertThat(result.completeListPasses()).isEqualTo(2);
        assertThat(transport.listCalls).isEqualTo(2);
    }

    @Test
    void exactOwnedResidueRemainingIsRetryableAndNeverCalledAbsent() throws Exception {
        FakeMultipartTransport transport = new FakeMultipartTransport();
        transport.leaveAfterAbort = true;
        var target = upload("cell-a/attempt/data", "still-present");
        transport.uploads.add(target);
        M5MultipartCleanupSessionV1 session = session(transport);
        Set<ObjectProviderTransport.MultipartUploadIdentity> owned = Set.of(target);

        var result = session.cleanupOwned(session.inventoryRoot(owned), owned);

        assertThat(result.kind()).isEqualTo(CleanupKindV1.EXACT_OWNED_RESIDUE_REMAINS);
        assertThat(result.completeListPasses()).isEqualTo(2);
        assertThat(transport.uploads).containsExactly(target);
    }

    @Test
    void repeatedContinuationTokenFailsClosedBeforeAbort() {
        FakeMultipartTransport transport = new FakeMultipartTransport();
        transport.pageSize = 1;
        transport.repeatContinuationToken = true;
        var first = upload("cell-a/attempt/data", "first");
        var second = upload("cell-a/attempt/data", "second");
        var third = upload("cell-a/attempt/data", "third");
        transport.uploads.addAll(Set.of(first, second, third));
        M5MultipartCleanupSessionV1 session = session(transport);
        Set<ObjectProviderTransport.MultipartUploadIdentity> owned = Set.of(first, second, third);

        assertThatThrownBy(() -> session.cleanupOwned(session.inventoryRoot(owned), owned))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("repeated token");
        assertThat(transport.abortCalls).isZero();
    }

    @Test
    void reboundInventoryRootAndForeignNamespaceAreRejectedBeforeList() {
        FakeMultipartTransport transport = new FakeMultipartTransport();
        M5MultipartCleanupSessionV1 session = session(transport);
        var target = upload("cell-a/attempt/data", "owned");
        Set<ObjectProviderTransport.MultipartUploadIdentity> owned = Set.of(target);

        assertThatThrownBy(() -> session.cleanupOwned(digest(9), owned))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("persisted root");
        assertThatThrownBy(() -> session.inventoryRoot(Set.of(upload("cell-b/attempt/data", "foreign"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exact Cell namespace");
        assertThat(transport.listCalls).isZero();
    }

    private static M5MultipartCleanupSessionV1 session(ObjectProviderTransport transport) {
        return new M5MultipartCleanupSessionV1(transport, scope(), "cell-a", 8, 64, 16_384, 1_024);
    }

    private static ObjectProviderTransport.MultipartUploadIdentity upload(String key, String id) {
        return new ObjectProviderTransport.MultipartUploadIdentity(key, bytes(id));
    }

    private static CellProviderScopeId scope() {
        return new CellProviderScopeId(digest(1));
    }

    private static Sha256Digest digest(int value) {
        byte[] raw = new byte[Sha256Digest.LENGTH];
        raw[raw.length - 1] = (byte) value;
        return Sha256Digest.copyOf(raw);
    }

    private static CanonicalBytes bytes(String value) {
        return CanonicalBytes.copyOf(value.getBytes(StandardCharsets.US_ASCII));
    }

    private static class BaseTransport implements ObjectProviderTransport {
        protected int listCalls;

        @Override
        public ObjectProviderCapabilities capabilities() {
            return new ObjectProviderCapabilities("fake-multipart", true, true, true, true, true, 1_024, 1_024, 16);
        }

        @Override
        public ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StreamingObject getRange(
                String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class FakeMultipartTransport extends BaseTransport {
        private final Set<MultipartUploadIdentity> uploads = new LinkedHashSet<>();
        private int pageSize = 16;
        private boolean repeatContinuationToken;
        private boolean unknownAfterAbort;
        private boolean leaveAfterAbort;
        private int abortCalls;

        @Override
        public MultipartCleanupCapabilities multipartCleanupCapabilities() {
            return new MultipartCleanupCapabilities(
                    "fake-multipart", "EXACT_UPLOAD_ID_ABORT_V1", true, true, true, 256, 256, 16);
        }

        @Override
        public MultipartListPage listMultipartUploads(
                String prefix, Optional<CanonicalBytes> continuationToken, int maximumUploads) {
            listCalls++;
            List<MultipartUploadIdentity> rows = uploads.stream()
                    .filter(upload -> upload.key().startsWith(prefix))
                    .sorted(Comparator.comparing(MultipartUploadIdentity::key)
                            .thenComparing(upload -> upload.uploadId().toHex()))
                    .toList();
            int start = continuationToken
                    .map(token -> Integer.parseInt(new String(token.toByteArray(), StandardCharsets.US_ASCII)))
                    .orElse(0);
            int end = Math.min(rows.size(), start + Math.min(pageSize, maximumUploads));
            Optional<CanonicalBytes> next = end < rows.size()
                    ? Optional.of(bytes(repeatContinuationToken ? "1" : Integer.toString(end)))
                    : Optional.empty();
            return new MultipartListPage(new ArrayList<>(rows.subList(start, end)), next);
        }

        @Override
        public ExactMultipartAbortResult abortMultipartUploadExact(String key, CanonicalBytes exactUploadId) {
            abortCalls++;
            MultipartUploadIdentity target = new MultipartUploadIdentity(key, exactUploadId);
            if (leaveAfterAbort) {
                return ExactMultipartAbortResult.ABORT_ACCEPTED;
            }
            boolean removed = uploads.remove(target);
            if (unknownAfterAbort) {
                return ExactMultipartAbortResult.RESPONSE_UNKNOWN;
            }
            return removed
                    ? ExactMultipartAbortResult.ABORT_ACCEPTED
                    : ExactMultipartAbortResult.DEFINITIVELY_NOT_FOUND;
        }
    }
}
