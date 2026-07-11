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
import java.util.concurrent.CompletableFuture;

public interface ObjectStore extends AutoCloseable {
    CompletableFuture<PutObjectResult> putObject(
            ObjectKey key,
            ByteBuffer payload,
            PutObjectOptions options);

    CompletableFuture<RangeReadResult> readRange(
            ObjectKey key,
            long offset,
            long length,
            RangeReadOptions options);

    CompletableFuture<HeadObjectResult> headObject(
            ObjectKey key,
            HeadObjectOptions options);

    @Override
    void close();
}
