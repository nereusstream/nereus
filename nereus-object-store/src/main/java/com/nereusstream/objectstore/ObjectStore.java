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

package com.nereusstream.objectstore;

import com.nereusstream.api.ObjectKey;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface ObjectStore extends AutoCloseable {
    /**
     * Stores one object. When {@link PutObjectOptions#ifAbsent()} is true and the key already
     * exists, the future fails with {@link ObjectAlreadyExistsException}.
     */
    default CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ByteBuffer payload,
            PutObjectOptions options) {
        ByteBufferObjectUpload source = new ByteBufferObjectUpload(payload);
        CompletableFuture<PutObjectResult> result;
        try {
            result = putObject(key, source, options);
        } catch (Throwable failure) {
            source.close();
            throw failure;
        }
        result.whenComplete((ignored, failure) -> source.close());
        return result;
    }

    default CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ReplayableObjectUpload source,
            PutObjectOptions options) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "this ObjectStore implementation does not support replayable uploads"));
    }

    default CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ReplayableObjectUpload source,
            PutObjectOptions options,
            PutObjectAttemptGuard attemptGuard) {
        return attemptGuard.authorize(key, 1).thenCompose(ignored -> putObject(key, source, options));
    }

    CompletableFuture<RangeReadResult> readRange(
            ObjectKey key,
            long offset,
            long length,
            RangeReadOptions options);

    CompletableFuture<HeadObjectResult> headObject(
            ObjectKey key,
            HeadObjectOptions options);

    default CompletableFuture<ListObjectsResult> listObjects(
            ObjectKeyPrefix prefix,
            Optional<String> continuationToken,
            ListObjectsOptions options) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "this ObjectStore implementation does not support object listing"));
    }

    default CompletableFuture<DeleteObjectResult> deleteObject(
            ObjectKey key,
            DeleteObjectOptions options) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "this ObjectStore implementation does not support object deletion"));
    }

    @Override
    void close();
}
