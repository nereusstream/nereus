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

package com.nereusstream.pulsar.offload;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Cell-scoped immutable Object provider seam for one sealed-ledger attempt. */
public interface PulsarOffloadObjectStoreV1 {
    record Capabilities(
            long maximumObjectBytes,
            long minimumPartBytes,
            long maximumPartBytes,
            int maximumPartCount,
            boolean immutableConditionalCreate,
            boolean streamingUpload,
            boolean boundedRangeRead,
            boolean boundedFullRead,
            boolean deterministicMultipartCleanup) {
        public Capabilities {
            if (maximumObjectBytes <= 0
                    || minimumPartBytes <= 0
                    || maximumPartBytes < minimumPartBytes
                    || maximumPartCount <= 0) {
                throw new IllegalArgumentException("provider capability number is invalid");
            }
        }
    }

    record ImmutableObject(String immutableVersion, long bytes, String sha256) {
        public ImmutableObject {
            Objects.requireNonNull(immutableVersion, "immutableVersion");
            Objects.requireNonNull(sha256, "sha256");
            if (immutableVersion.isEmpty() || bytes <= 0 || !sha256.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("immutable Object proof is invalid");
            }
        }
    }

    record Body(long bytes, String sha256, InputStreamFactory inputStreamFactory) {
        public Body {
            Objects.requireNonNull(sha256, "sha256");
            Objects.requireNonNull(inputStreamFactory, "inputStreamFactory");
            if (bytes <= 0 || !sha256.matches("[0-9a-f]{64}")) {
                throw new IllegalArgumentException("Object body descriptor is invalid");
            }
        }
    }

    @FunctionalInterface
    interface InputStreamFactory {
        InputStream open() throws IOException;
    }

    Capabilities capabilities();

    CompletionStage<ImmutableObject> createImmutable(String key, Body body);

    CompletionStage<ImmutableObject> head(String key);

    CompletionStage<byte[]> readRange(String key, long offset, int length);

    CompletionStage<Void> deleteAndProveAbsent(String key);

    CompletionStage<Void> cleanupAttemptMultipartResidue(String attemptPrefix);

    CompletionStage<Void> close();
}
