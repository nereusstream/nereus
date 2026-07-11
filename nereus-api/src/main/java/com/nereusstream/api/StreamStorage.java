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

    CompletableFuture<ReadResult> read(
            StreamId streamId,
            long startOffset,
            ReadOptions options);

    CompletableFuture<ResolveResult> resolve(
            StreamId streamId,
            long startOffset,
            ResolveOptions options);

    CompletableFuture<Void> trim(
            StreamId streamId,
            long beforeOffset,
            TrimOptions options);

    CompletableFuture<StreamMetadata> getStreamMetadata(StreamId streamId);

    @Override
    void close();
}
