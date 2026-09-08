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

package com.nereusstream.storage.object.gc;

import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExactExternalIdentityV1;
import com.nereusstream.storage.object.gc.M5TargetDeleteAuthorityRecordsV1.ExternalIdentityObservationV1;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Re-reads a physical target using its actual provider and native namespace binding.
 *
 * <p>PRESENT requires complete exact immutable identity equality against the persisted observation. ABSENT
 * requires authoritative native absence (including complete bounded inventory where the provider requires it).
 * Changed identity, partial reads, timeouts and missing adapters fail. Implementations must not return caller
 * assertions or cached observations. The coordinator invokes this port between native authority validations;
 * a successful read alone grants no permission to dispatch a delete.
 */
@FunctionalInterface
public interface DeleteExternalIdentityReaderV2 {
    CompletionStage<ExternalIdentityObservationV1> rereadExact(ExactExternalIdentityV1 expected);

    static DeleteExternalIdentityReaderV2 unsupported() {
        return expected -> CompletableFuture.failedFuture(
                new UnsupportedOperationException("native external deletion identity reader is not installed"));
    }
}
