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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

/** Borrowed stateless Provider transport. Cell sessions never close or mutate this shared object. */
public interface ObjectProviderTransport {
    ObjectProviderCapabilities capabilities();

    /** M5-D delete capabilities are additive; an M3 C1 adapter is unsupported until explicitly admitted. */
    default ObjectDeleteCapabilities deleteCapabilities() {
        return ObjectDeleteCapabilities.unsupported(capabilities().providerIdentity());
    }

    /** M5-D multipart cleanup is a separate additive capability and defaults to no authority and no I/O. */
    default MultipartCleanupCapabilities multipartCleanupCapabilities() {
        return MultipartCleanupCapabilities.unsupported(capabilities().providerIdentity());
    }

    ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException;

    StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException;

    StreamingObject getRange(String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
            throws IOException;

    ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) throws IOException;

    /**
     * Deletes only the exact immutable Provider version named by {@code exactVersionToken}.
     *
     * <p>Callers must perform the M5-D full-body identity read and the post-delete complete LIST/full-GET absence
     * reconciliation. A default C1 transport performs no I/O and remains unsupported.
     */
    default ConditionalDeleteResult deleteExactVersion(String key, CanonicalBytes exactVersionToken)
            throws IOException {
        return ConditionalDeleteResult.UNSUPPORTED;
    }

    /** Lists incomplete multipart uploads using an exact adapter-owned continuation token. */
    default MultipartListPage listMultipartUploads(
            String prefix, Optional<CanonicalBytes> continuationToken, int maximumUploads) throws IOException {
        throw new UnsupportedOperationException("multipart cleanup is unsupported");
    }

    /**
     * Requests abort of only the exact key/upload-id pair.
     *
     * <p>The response never proves absence. M5-D callers must perform a complete bounded multipart relist after
     * every result, including response loss and definitive failures.
     */
    default ExactMultipartAbortResult abortMultipartUploadExact(String key, CanonicalBytes exactUploadId)
            throws IOException {
        return ExactMultipartAbortResult.UNSUPPORTED;
    }

    /** Adapter-owned typed classification; unknown exceptions remain fatal and are never interpreted as absence. */
    default FailureKind classifyFailure(IOException failure) {
        return FailureKind.FATAL;
    }

    enum FailureKind {
        NOT_FOUND,
        RETRYABLE,
        OUTCOME_UNKNOWN,
        FATAL
    }

    enum ConditionalCreateResult {
        CREATED,
        ALREADY_EXISTS,
        DEFINITIVE_CONFLICT,
        RESPONSE_UNKNOWN
    }

    enum ConditionalDeleteResult {
        DELETED_EXACT,
        DEFINITIVELY_NOT_FOUND,
        VERSION_PRECONDITION_FAILED,
        RETRYABLE,
        RESPONSE_UNKNOWN,
        DEFINITIVE_CONFLICT,
        UNSUPPORTED
    }

    enum ExactMultipartAbortResult {
        ABORT_ACCEPTED,
        DEFINITIVELY_NOT_FOUND,
        RETRYABLE,
        RESPONSE_UNKNOWN,
        DEFINITIVE_CONFLICT,
        UNSUPPORTED
    }

    /** Exact version-delete admission snapshot; this value itself grants no delete authority. */
    record ObjectDeleteCapabilities(
            String providerIdentity,
            String mechanism,
            boolean exactImmutableVersionDelete,
            boolean typedResponseLoss,
            boolean strongGetListReconciliation,
            int maximumVersionTokenBytes) {
        public ObjectDeleteCapabilities {
            if (providerIdentity == null
                    || providerIdentity.isBlank()
                    || mechanism == null
                    || mechanism.isBlank()
                    || maximumVersionTokenBytes < 0) {
                throw new IllegalArgumentException("Object delete capabilities are invalid");
            }
            if (exactImmutableVersionDelete
                    != (typedResponseLoss && strongGetListReconciliation && maximumVersionTokenBytes > 0)) {
                throw new IllegalArgumentException("Object delete capability facts are inconsistent");
            }
        }

        public static ObjectDeleteCapabilities unsupported(String providerIdentity) {
            return new ObjectDeleteCapabilities(providerIdentity, "UNSUPPORTED", false, false, false, 0);
        }

        public void requireVersionMatchDeleteV1() {
            if (!"VERSION_MATCH_DELETE_V1".equals(mechanism) || !exactImmutableVersionDelete) {
                throw new IllegalArgumentException("Provider does not satisfy VERSION_MATCH_DELETE_V1");
            }
        }
    }

    /** Exact upload-id abort and complete ordered listing admission; this value grants no dispatch authority. */
    record MultipartCleanupCapabilities(
            String providerIdentity,
            String mechanism,
            boolean exactUploadIdAbort,
            boolean completeOrderedListing,
            boolean typedResponseLoss,
            int maximumUploadIdBytes,
            int maximumContinuationTokenBytes,
            int maximumListPageUploads) {
        public MultipartCleanupCapabilities {
            if (providerIdentity == null
                    || providerIdentity.isBlank()
                    || mechanism == null
                    || mechanism.isBlank()
                    || maximumUploadIdBytes < 0
                    || maximumContinuationTokenBytes < 0
                    || maximumListPageUploads < 0) {
                throw new IllegalArgumentException("multipart cleanup capabilities are invalid");
            }
            boolean admitted = exactUploadIdAbort && completeOrderedListing && typedResponseLoss;
            if (admitted
                    != (maximumUploadIdBytes > 0 && maximumContinuationTokenBytes > 0 && maximumListPageUploads > 0)) {
                throw new IllegalArgumentException("multipart cleanup capability facts are inconsistent");
            }
        }

        public static MultipartCleanupCapabilities unsupported(String providerIdentity) {
            return new MultipartCleanupCapabilities(providerIdentity, "UNSUPPORTED", false, false, false, 0, 0, 0);
        }

        public void requireExactUploadIdAbortV1() {
            if (!"EXACT_UPLOAD_ID_ABORT_V1".equals(mechanism)
                    || !exactUploadIdAbort
                    || !completeOrderedListing
                    || !typedResponseLoss) {
                throw new IllegalArgumentException("Provider does not satisfy EXACT_UPLOAD_ID_ABORT_V1");
            }
        }
    }

    /** Exact immutable identity for one incomplete multipart upload. */
    record MultipartUploadIdentity(String key, CanonicalBytes uploadId) {
        public MultipartUploadIdentity {
            if (key == null || key.isEmpty() || key.indexOf('\0') >= 0) {
                throw new IllegalArgumentException("multipart upload key is invalid");
            }
            if (uploadId == null || uploadId.isEmpty()) {
                throw new IllegalArgumentException("multipart upload id is empty");
            }
            uploadId = CanonicalBytes.copyOf(uploadId.toByteArray());
        }
    }

    record MultipartListPage(List<MultipartUploadIdentity> uploads, Optional<CanonicalBytes> nextContinuationToken) {
        public MultipartListPage {
            uploads = List.copyOf(uploads);
            nextContinuationToken = nextContinuationToken.map(value -> CanonicalBytes.copyOf(value.toByteArray()));
            if (nextContinuationToken.isPresent()
                    && nextContinuationToken.orElseThrow().isEmpty()) {
                throw new IllegalArgumentException("multipart continuation token must be non-empty");
            }
        }
    }

    record StreamingObject(
            long bodyLength,
            long inclusiveStart,
            long exclusiveEnd,
            Optional<CanonicalBytes> immutableVersionToken,
            InputStream body)
            implements AutoCloseable {
        public StreamingObject {
            if (bodyLength <= 0 || inclusiveStart < 0 || exclusiveEnd <= inclusiveStart || exclusiveEnd > bodyLength) {
                throw new IllegalArgumentException("stream range is invalid");
            }
            immutableVersionToken = immutableVersionToken.map(value -> CanonicalBytes.copyOf(value.toByteArray()));
            if (body == null) {
                throw new NullPointerException("body");
            }
        }

        @Override
        public void close() throws IOException {
            body.close();
        }
    }

    record ListedObject(String key, long bodyLength, Optional<CanonicalBytes> immutableVersionToken) {
        public ListedObject {
            if (key == null || key.isEmpty() || bodyLength <= 0) {
                throw new IllegalArgumentException("listed Object is invalid");
            }
            immutableVersionToken = immutableVersionToken.map(value -> CanonicalBytes.copyOf(value.toByteArray()));
        }
    }

    record ListPage(List<ListedObject> objects, Optional<CanonicalBytes> nextContinuationToken) {
        public ListPage {
            objects = List.copyOf(objects);
            nextContinuationToken = nextContinuationToken.map(value -> CanonicalBytes.copyOf(value.toByteArray()));
            if (nextContinuationToken.isPresent()
                    && nextContinuationToken.orElseThrow().isEmpty()) {
                throw new IllegalArgumentException("continuation token must be non-empty");
            }
        }
    }
}
