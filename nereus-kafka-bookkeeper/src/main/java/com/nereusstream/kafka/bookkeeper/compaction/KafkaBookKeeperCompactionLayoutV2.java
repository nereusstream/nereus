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
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.PartKind;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.PartPlan;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaBookKeeperInventoryV2.Task;
import com.nereusstream.kafka.bookkeeper.compaction.KafkaCompactionRecordsV1.CompactionPlan;
import com.nereusstream.storage.api.bookkeeper.BookKeeperCapabilitySnapshotV1;
import com.nereusstream.storage.api.lifecycle.PhysicalResourceIdV2.Namespace;
import com.nereusstream.storage.object.materialization.M5MaterializationCodecV1;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Deterministic BK entry segmentation of retained Kafka batches and all eight rebuilt indexes, before allocation. */
public final class KafkaBookKeeperCompactionLayoutV2 {
    private static final int MAGIC = 0x4b424345;
    public static final int ENVELOPE_BYTES = 91;
    public static final int MAX_ARTIFACT_BYTES = 67_108_864;

    public record Chunk(
            PartKind kind,
            Sha256Digest semanticOutputSha256,
            int artifactOrdinal,
            int chunkOrdinal,
            int chunkCount,
            int artifactLength,
            Sha256Digest artifactSha256,
            CanonicalBytes body) {
        public Chunk {
            Objects.requireNonNull(kind, "kind");
            KafkaBookKeeperInventoryV2.requireDigest(semanticOutputSha256);
            KafkaBookKeeperInventoryV2.requireDigest(artifactSha256);
            Objects.requireNonNull(body, "body");
            if (artifactOrdinal < 0
                    || artifactOrdinal >= KafkaCompactionRecordsV1.MAX_BATCHES
                    || (kind == PartKind.INDEX && artifactOrdinal >= 8)
                    || chunkOrdinal < 0
                    || chunkCount <= chunkOrdinal
                    || chunkCount > KafkaBookKeeperInventoryV2.MAX_PART_ENTRIES
                    || artifactLength <= 0
                    || artifactLength > MAX_ARTIFACT_BYTES
                    || body.length() == 0
                    || body.length() > artifactLength
                    || chunkCount > artifactLength) {
                throw new IllegalArgumentException("BK compaction chunk exceeds its exact artifact bounds");
            }
        }
    }

    public record PartBody(PartKind kind, List<CanonicalBytes> entries) {
        public PartBody {
            Objects.requireNonNull(kind, "kind");
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
            if (entries.isEmpty() || entries.size() > KafkaBookKeeperInventoryV2.MAX_PART_ENTRIES) {
                throw new IllegalArgumentException("BK compaction part entry count exceeds bound");
            }
        }

        public PartPlan plan() {
            return new PartPlan(
                    kind,
                    entries.size(),
                    entries.stream().mapToLong(CanonicalBytes::length).sum(),
                    entriesRoot(entries));
        }
    }

    /** Persist only these output entries. The input batches used by the semantic compiler are not carrier output. */
    public record Layout(Task task, List<PartBody> parts) {
        public Layout {
            Objects.requireNonNull(task, "task");
            parts = List.copyOf(Objects.requireNonNull(parts, "parts"));
            if (!parts.stream().map(PartBody::plan).toList().equals(task.parts())) {
                throw new IllegalArgumentException("BK compaction layout differs from its inventory task");
            }
            for (PartBody part : parts) {
                for (CanonicalBytes entry : part.entries()) {
                    Chunk chunk = decodeChunk(entry, task.capability().maximumAddPayloadBytes());
                    if (chunk.kind() != part.kind()
                            || !chunk.semanticOutputSha256().equals(task.semanticOutputSha256())) {
                        throw new IllegalArgumentException("BK compaction entry belongs to a different output or part");
                    }
                }
            }
        }
    }

    private KafkaBookKeeperCompactionLayoutV2() {}

    public static Layout plan(
            CompactionPlan plan,
            KafkaCompactionSemanticOutputV2 semantic,
            Namespace namespace,
            BookKeeperCapabilitySnapshotV1 capability,
            long attempt,
            int maximumEntryBytes,
            long maximumPartBytes) {
        if (maximumEntryBytes <= ENVELOPE_BYTES
                || maximumEntryBytes > capability.maximumAddPayloadBytes()
                || maximumPartBytes < maximumEntryBytes
                || maximumPartBytes > KafkaBookKeeperInventoryV2.MAX_PART_BYTES) {
            throw new IllegalArgumentException("BK compaction layout exceeds admitted entry or part limits");
        }
        var proof = new KafkaCompactionSemanticValidatorV1().validateSemantic(plan, semantic);
        List<PartBody> parts = new ArrayList<>();
        List<CanonicalBytes> data = semantic.batchOutputs().stream()
                .flatMap(value -> value.outputBody().stream())
                .toList();
        appendParts(parts, PartKind.DATA, data, semantic.outputIdentitySha256(), maximumEntryBytes, maximumPartBytes);
        appendParts(
                parts,
                PartKind.INDEX,
                semantic.indexBodies(),
                semantic.outputIdentitySha256(),
                maximumEntryBytes,
                maximumPartBytes);
        Task task = new Task(
                M5MaterializationCodecV1.encodeSourceCut(plan.sourceCut()),
                semantic.compactionPlanRootSha256(),
                semantic.outputIdentitySha256(),
                proof.semanticValidationRootSha256(),
                namespace,
                capability,
                attempt,
                parts.stream().map(PartBody::plan).toList());
        return new Layout(task, parts);
    }

    private static void appendParts(
            List<PartBody> parts,
            PartKind kind,
            List<CanonicalBytes> artifacts,
            Sha256Digest outputId,
            int maximumEntryBytes,
            long maximumPartBytes) {
        List<CanonicalBytes> entries = new ArrayList<>();
        long partLength = 0;
        int chunkBytes = maximumEntryBytes - ENVELOPE_BYTES;
        for (int ordinal = 0; ordinal < artifacts.size(); ordinal++) {
            CanonicalBytes artifact = artifacts.get(ordinal);
            if (artifact.length() == 0 || artifact.length() > MAX_ARTIFACT_BYTES) {
                throw new IllegalArgumentException("BK compaction artifact exceeds bound");
            }
            Sha256Digest digest = Sha256Digest.hash(artifact);
            byte[] bytes = artifact.toByteArray();
            int count = Math.toIntExact((bytes.length + (long) chunkBytes - 1) / chunkBytes);
            for (int chunk = 0; chunk < count; chunk++) {
                int start = Math.multiplyExact(chunk, chunkBytes);
                CanonicalBytes body = CanonicalBytes.copyOf(
                        Arrays.copyOfRange(bytes, start, Math.min(bytes.length, Math.addExact(start, chunkBytes))));
                CanonicalBytes entry =
                        encodeChunk(new Chunk(kind, outputId, ordinal, chunk, count, bytes.length, digest, body));
                if (!entries.isEmpty()
                        && (partLength + entry.length() > maximumPartBytes
                                || entries.size() == KafkaBookKeeperInventoryV2.MAX_PART_ENTRIES)) {
                    addPart(parts, new PartBody(kind, entries));
                    entries = new ArrayList<>();
                    partLength = 0;
                }
                entries.add(entry);
                partLength += entry.length();
            }
        }
        if (!entries.isEmpty()) {
            addPart(parts, new PartBody(kind, entries));
        }
    }

    private static void addPart(List<PartBody> parts, PartBody part) {
        if (parts.size() == KafkaBookKeeperInventoryV2.MAX_PARTS) {
            throw new IllegalArgumentException("BK compaction layout exceeds part count bound");
        }
        parts.add(part);
    }

    public static CanonicalBytes encodeChunk(Chunk chunk) {
        ByteBuffer output =
                ByteBuffer.allocate(Math.addExact(ENVELOPE_BYTES, chunk.body().length()));
        output.putInt(MAGIC).putShort((short) 2).put((byte) (chunk.kind() == PartKind.DATA ? 1 : 2));
        output.put(chunk.semanticOutputSha256().bytes().toByteArray());
        output.putInt(chunk.artifactOrdinal()).putInt(chunk.chunkOrdinal()).putInt(chunk.chunkCount());
        output.putInt(chunk.artifactLength()).putInt(chunk.body().length());
        output.put(chunk.artifactSha256().bytes().toByteArray())
                .put(chunk.body().toByteArray());
        return CanonicalBytes.copyOf(output.array());
    }

    public static Chunk decodeChunk(CanonicalBytes bytes, int maximumEntryBytes) {
        if (bytes.length() <= ENVELOPE_BYTES || bytes.length() > maximumEntryBytes) {
            throw new IllegalArgumentException("BK compaction entry exceeds admitted bound");
        }
        ByteBuffer input = ByteBuffer.wrap(bytes.toByteArray());
        if (input.getInt() != MAGIC || input.getShort() != 2) {
            throw new IllegalArgumentException("BK compaction entry preamble differs");
        }
        PartKind kind =
                switch (input.get()) {
                    case 1 -> PartKind.DATA;
                    case 2 -> PartKind.INDEX;
                    default -> throw new IllegalArgumentException("unknown BK compaction entry kind");
                };
        Sha256Digest outputId = readDigest(input);
        int ordinal = input.getInt();
        int chunk = input.getInt();
        int count = input.getInt();
        int length = input.getInt();
        int bodyLength = input.getInt();
        Sha256Digest digest = readDigest(input);
        if (bodyLength != input.remaining()) {
            throw new IllegalArgumentException("BK compaction entry body length differs");
        }
        byte[] body = new byte[bodyLength];
        input.get(body);
        return new Chunk(kind, outputId, ordinal, chunk, count, length, digest, CanonicalBytes.copyOf(body));
    }

    public static Sha256Digest entriesRoot(List<CanonicalBytes> entries) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(MAGIC);
                output.writeShort(2);
                output.writeByte(3);
                output.writeInt(entries.size());
                for (CanonicalBytes entry : entries) {
                    output.writeInt(entry.length());
                    output.write(Sha256Digest.hash(entry).bytes().toByteArray());
                }
            }
            return Sha256Digest.hash(CanonicalBytes.copyOf(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory BK entries root encoding failed", impossible);
        }
    }

    private static Sha256Digest readDigest(ByteBuffer input) {
        byte[] digest = new byte[Sha256Digest.LENGTH];
        input.get(digest);
        return Sha256Digest.copyOf(digest);
    }
}
