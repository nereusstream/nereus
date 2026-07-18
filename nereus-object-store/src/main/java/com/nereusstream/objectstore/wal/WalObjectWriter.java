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

package com.nereusstream.objectstore.wal;

import com.nereusstream.objectstore.PutObjectAttemptGuard;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public interface WalObjectWriter {
    PreparedWalObject prepare(WalWriteRequest request);

    CompletableFuture<WalWriteResult> upload(PreparedWalObject preparedObject);

    /**
     * Uploads through a provider-attempt guard. Implementations with internal retries must invoke the guard before
     * every provider transmission; the default preserves compatibility for single-attempt test writers.
     */
    default CompletableFuture<WalWriteResult> upload(
            PreparedWalObject preparedObject,
            PutObjectAttemptGuard attemptGuard) {
        PreparedWalObject prepared = Objects.requireNonNull(
                preparedObject, "preparedObject");
        PutObjectAttemptGuard guard = Objects.requireNonNull(
                attemptGuard, "attemptGuard");
        return guard.authorize(prepared.result().objectKey(), 1)
                .thenCompose(ignored -> upload(prepared));
    }

    default CompletableFuture<WalWriteResult> write(WalWriteRequest request) {
        try {
            return upload(prepare(request));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
