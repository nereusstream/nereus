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

package com.nereusstream.storage.object.extent;

/** Allocation-free checked byte/chunk counter used by local and Provider streaming paths. */
public final class CheckedStreamingCounter {
    private final long maximumBytes;
    private long bytes;
    private long chunks;

    public CheckedStreamingCounter(long maximumBytes) {
        if (maximumBytes <= 0) {
            throw new IllegalArgumentException("streaming byte maximum must be positive");
        }
        this.maximumBytes = maximumBytes;
    }

    public void charge(long chunkBytes) {
        if (chunkBytes <= 0) {
            throw new IllegalArgumentException("streaming chunk length must be positive");
        }
        long nextBytes = Math.addExact(bytes, chunkBytes);
        long nextChunks = Math.incrementExact(chunks);
        if (nextBytes > maximumBytes) {
            throw new IllegalArgumentException("streaming byte count exceeds its admitted maximum");
        }
        bytes = nextBytes;
        chunks = nextChunks;
    }

    public Snapshot snapshot() {
        return new Snapshot(bytes, chunks, maximumBytes);
    }

    public record Snapshot(long bytes, long chunks, long maximumBytes) {}
}
