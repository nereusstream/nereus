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

package com.nereusstream.api;

import java.util.concurrent.CompletableFuture;

/** Protocol-neutral L0 storage API. */
public interface StreamStorage extends AutoCloseable {
    CompletableFuture<StreamMetadata> createOrGetStream(
            StreamName streamName,
            StreamCreateOptions options);

    CompletableFuture<AppendSession> acquireAppendSession(
            StreamId streamId,
            AppendSessionOptions options);

    CompletableFuture<AppendResult> append(
            StreamId streamId,
            AppendBatch batch,
            AppendOptions options);

    /**
     * Appends with an optional caller-visible logical-offset precondition.
     *
     * <p>The default preserves binary compatibility for existing providers. Providers that do not implement
     * conditional append fail closed rather than silently ignoring a non-empty precondition.
     */
    default CompletableFuture<AppendResult> append(
            StreamId streamId,
            AppendBatch batch,
            AppendOptions options,
            AppendPrecondition precondition) {
        if (precondition == null) {
            return NereusException.failedFuture(
                    ErrorCode.INVALID_ARGUMENT, false, "append precondition is required");
        }
        if (precondition.equals(AppendPrecondition.none())) {
            return append(streamId, batch, options);
        }
        return NereusException.failedFuture(
                ErrorCode.UNSUPPORTED_APPEND_PRECONDITION,
                false,
                "storage provider does not support conditional append");
    }

    CompletableFuture<AppendResult> recoverAppend(
            StreamId streamId,
            AppendAttemptId attemptId,
            AppendRecoveryOptions options);

    CompletableFuture<ReadResult> read(
            StreamId streamId,
            long startOffset,
            ReadOptions options);

    /**
     * Reads through the public semantic-view contract.
     *
     * <p>Existing providers can serve the exact legacy request through their original method. New boundary, view,
     * or first-entry semantics require an explicit provider implementation and otherwise fail closed.
     */
    default CompletableFuture<SemanticReadResult> read(
            StreamId streamId,
            ReadRequest request) {
        if (request == null) {
            return NereusException.failedFuture(
                    ErrorCode.INVALID_ARGUMENT, false, "read request is required");
        }
        if (!request.isLegacyEquivalent()) {
            return NereusException.failedFuture(
                    ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                    false,
                    "storage provider does not support the requested read semantics");
        }
        return read(streamId, request.startOffset(), request.options())
                .thenApply(result -> SemanticReadResult.forRequest(
                        request, result, result.nextOffset()));
    }

    CompletableFuture<ResolveResult> resolve(
            StreamId streamId,
            long startOffset,
            ResolveOptions options);

    CompletableFuture<Void> trim(
            StreamId streamId,
            long beforeOffset,
            TrimOptions options);

    CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId);

    CompletableFuture<StreamMetadata> seal(StreamId streamId, SealOptions options);

    CompletableFuture<StreamMetadata> delete(StreamId streamId, DeleteOptions options);

    @Override
    void close();
}
