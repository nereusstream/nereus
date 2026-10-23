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

package com.nereusstream.storage.object.materialization;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Optional;

/** Per-entry data contract and incremental commitment. This does not qualify the reader's native authority. */
public final class M5PulsarEntriesV2 {
    private M5PulsarEntriesV2() {}

    public record Entry(M5PulsarCoverageV2.Position position, CanonicalBytes payload, long timestamp) {
        public Entry {
            Objects.requireNonNull(position, "position");
            payload = CanonicalBytes.copyOf(
                    Objects.requireNonNull(payload, "payload").toByteArray());
            if (timestamp < 0) {
                throw new IllegalArgumentException("Pulsar entry timestamp is unavailable");
            }
        }
    }

    /** One owned entry at a time; close must dispose the original reader, never merely its observer. */
    public interface Cursor extends AutoCloseable {
        Optional<Entry> next() throws IOException;

        @Override
        void close() throws IOException;
    }

    public static final class Commitment {
        private final M5PulsarCoverageV2 coverage;
        private final MessageDigest sha;
        private Optional<M5PulsarCoverageV2.Position> next;
        private long count;
        private boolean finished;
        private long minimumTimestamp = Long.MAX_VALUE;
        private long maximumTimestamp = -1;

        public Commitment(M5PulsarCoverageV2 coverage) {
            this.coverage = Objects.requireNonNull(coverage, "coverage");
            try {
                sha = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("JDK has no SHA-256", impossible);
            }
            byte[] domain = "NEREUS_V2_M5_PULSAR_ENTRY_ROOT_V2".getBytes(StandardCharsets.US_ASCII);
            sha.update(ByteBuffer.allocate(4).putInt(domain.length).array());
            sha.update(domain);
            sha.update(ByteBuffer.allocate(8).putLong(coverage.entryCount()).array());
            next = Optional.of(coverage.firstPosition());
        }

        public void accept(Entry entry) {
            acceptDigest(
                    entry.position(), entry.payload().length(), Sha256Digest.hash(entry.payload()), entry.timestamp());
        }

        /** Codec uses only the digest it just measured over all bytes of this exact entry. */
        void acceptDigest(M5PulsarCoverageV2.Position position, int length, Sha256Digest payloadSha, long timestamp) {
            if (finished || next.isEmpty() || !next.orElseThrow().equals(position) || length < 0 || timestamp < 0) {
                throw new IllegalStateException("Pulsar entry sequence differs from the exact captured cut");
            }
            sha.update(ByteBuffer.allocate(20)
                    .putLong(position.ledgerId())
                    .putLong(position.entryId())
                    .putInt(length)
                    .array());
            sha.update(payloadSha.bytes().toByteArray());
            count = Math.addExact(count, 1);
            minimumTimestamp = Math.min(minimumTimestamp, timestamp);
            maximumTimestamp = Math.max(maximumTimestamp, timestamp);
            next = coverage.firstAtOrAfter(
                    new M5PulsarCoverageV2.Position(position.ledgerId(), Math.addExact(position.entryId(), 1)));
        }

        public Sha256Digest finish() {
            if (finished || next.isPresent() || count != coverage.entryCount()) {
                throw new IllegalStateException("Pulsar entry sequence is incomplete or already finalized");
            }
            finished = true;
            return Sha256Digest.copyOf(sha.digest());
        }

        public void requireTimestampSummary(long minimum, long maximum) {
            if (minimumTimestamp != minimum || maximumTimestamp != maximum) {
                throw new IllegalStateException("Pulsar timestamp summary differs from its original entries");
            }
        }
    }
}
