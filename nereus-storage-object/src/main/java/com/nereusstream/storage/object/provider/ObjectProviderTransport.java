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

    ConditionalCreateResult putIfAbsent(ObjectIdentity identity, InputStream body) throws IOException;

    StreamingObject get(String key, Optional<CanonicalBytes> exactVersionToken) throws IOException;

    StreamingObject getRange(String key, long inclusiveStart, long exclusiveEnd, Optional<CanonicalBytes> versionToken)
            throws IOException;

    ListPage list(String prefix, Optional<CanonicalBytes> continuationToken, int maximumKeys) throws IOException;

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
