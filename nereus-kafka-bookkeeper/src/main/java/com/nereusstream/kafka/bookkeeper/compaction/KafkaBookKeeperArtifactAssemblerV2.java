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

package com.nereusstream.kafka.bookkeeper.compaction;

import com.nereusstream.domain.bytes.CanonicalBytes;
import com.nereusstream.domain.bytes.Sha256Digest;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionLayoutV2.Chunk;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperCompactionLayoutV2.PartBody;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.PartKind;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Task;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.Gap;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaSealedBookKeeperDescriptorV2.IndexLocator;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Reassembles ordered BK chunks with exact artifact boundaries; used both before publication and after restart. */
final class KafkaBookKeeperArtifactAssemblerV2 {
    record Artifacts(List<CanonicalBytes> batches, List<CanonicalBytes> indexes, List<IndexLocator> indexLocators) {
        Artifacts {
            batches = List.copyOf(batches);
            indexes = List.copyOf(indexes);
            indexLocators = List.copyOf(indexLocators);
        }
    }

    private KafkaBookKeeperArtifactAssemblerV2() {}

    static Artifacts assemble(Task task, List<PartBody> parts) {
        if (!parts.stream().map(PartBody::plan).toList().equals(task.parts())) {
            throw new IllegalArgumentException("BK artifact parts differ from their exact task roots");
        }
        List<CanonicalBytes> batches = new ArrayList<>();
        List<CanonicalBytes> indexes = new ArrayList<>();
        List<IndexLocator> locators = new ArrayList<>();
        ByteArrayOutputStream body = null;
        Chunk first = null;
        IndexLocator locator = null;
        int nextChunk = 0;
        boolean indexPhase = false;
        for (int ordinal = 0; ordinal < parts.size(); ordinal++) {
            PartBody part = parts.get(ordinal);
            for (int entry = 0; entry < part.entries().size(); entry++) {
                Chunk chunk = KafkaBookKeeperCompactionLayoutV2.decodeChunk(
                        part.entries().get(entry), task.capability().maximumAddPayloadBytes());
                if (chunk.kind() != part.kind() || !chunk.semanticOutputSha256().equals(task.semanticOutputSha256())) {
                    throw new IllegalArgumentException("BK artifact chunk differs from its exact task or part kind");
                }
                if (chunk.kind() == PartKind.INDEX) {
                    indexPhase = true;
                } else if (indexPhase) {
                    throw new IllegalArgumentException("BK data artifacts follow index artifacts");
                }
                List<CanonicalBytes> target = chunk.kind() == PartKind.DATA ? batches : indexes;
                if (body == null) {
                    if (chunk.chunkOrdinal() != 0 || chunk.artifactOrdinal() != target.size()) {
                        throw new IllegalArgumentException(
                                "BK artifact starts with a missing, duplicate or reordered chunk");
                    }
                    first = chunk;
                    body = new ByteArrayOutputStream();
                    nextChunk = 0;
                    locator = chunk.kind() == PartKind.INDEX
                            ? new IndexLocator(
                                    ordinal, entry, chunk.chunkCount(), chunk.artifactLength(), chunk.artifactSha256())
                            : null;
                }
                if (chunk.kind() != first.kind()
                        || chunk.artifactOrdinal() != first.artifactOrdinal()
                        || chunk.chunkOrdinal() != nextChunk
                        || chunk.chunkCount() != first.chunkCount()
                        || chunk.artifactLength() != first.artifactLength()
                        || !chunk.artifactSha256().equals(first.artifactSha256())
                        || body.size() + (long) chunk.body().length() > first.artifactLength()) {
                    throw new IllegalArgumentException("BK artifact chunk identity, order or length differs");
                }
                body.writeBytes(chunk.body().toByteArray());
                nextChunk++;
                if (nextChunk == first.chunkCount()) {
                    CanonicalBytes value = CanonicalBytes.copyOf(body.toByteArray());
                    if (value.length() != first.artifactLength()
                            || !Sha256Digest.hash(value).equals(first.artifactSha256())) {
                        throw new IllegalArgumentException("BK artifact checksum or complete length differs");
                    }
                    target.add(value);
                    if (locator != null) {
                        locators.add(locator);
                    }
                    body = null;
                }
            }
        }
        if (body != null || indexes.size() != 8) {
            throw new IllegalArgumentException("BK artifacts end with incomplete chunks or indexes");
        }
        return new Artifacts(batches, indexes, locators);
    }

    static Sha256Digest gapRoot(List<Gap> gaps) {
        ByteBuffer bytes = ByteBuffer.allocate(Math.addExact(10, Math.multiplyExact(gaps.size(), 16)));
        bytes.putInt(0x4b424752).putShort((short) 2).putInt(gaps.size());
        for (Gap gap : gaps) {
            bytes.putLong(gap.inclusiveStart()).putLong(gap.exclusiveEnd());
        }
        return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.array()));
    }
}
