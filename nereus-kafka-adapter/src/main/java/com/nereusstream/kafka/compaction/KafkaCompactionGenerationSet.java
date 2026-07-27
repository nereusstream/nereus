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

package com.nereusstream.kafka.compaction;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.materialization.GenerationCommitResult;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/** Canonical gap-free set of committed NTC2 generation identities activated by one binding CAS. */
public record KafkaCompactionGenerationSet(
    StreamId streamId,
    OffsetRange coverage,
    List<GenerationCommitResult> generations,
    Checksum digestSha256) {
  public static final int MAX_GENERATIONS = 128;

  public KafkaCompactionGenerationSet {
    Objects.requireNonNull(streamId, "streamId");
    Objects.requireNonNull(coverage, "coverage");
    generations = List.copyOf(Objects.requireNonNull(generations, "generations"));
    Objects.requireNonNull(digestSha256, "digestSha256");
    if (coverage.isEmpty()
        || generations.isEmpty()
        || generations.size() > MAX_GENERATIONS
        || digestSha256.type() != ChecksumType.SHA256) {
      throw new IllegalArgumentException("invalid Kafka compaction generation set");
    }
    long cursor = coverage.startOffset();
    for (GenerationCommitResult generation : generations) {
      if (!generation.streamId().equals(streamId)
          || generation.view() != ReadView.TOPIC_COMPACTED
          || generation.coverage().startOffset() != cursor) {
        throw new IllegalArgumentException(
            "Kafka compaction generations must be same-stream, same-view and gap-free");
      }
      cursor = generation.coverage().endOffset();
    }
    if (cursor != coverage.endOffset()
        || !digestSha256.equals(canonicalDigest(streamId, coverage, generations))) {
      throw new IllegalArgumentException(
          "Kafka compaction generation set coverage/digest is inconsistent");
    }
  }

  public static KafkaCompactionGenerationSet initial(GenerationCommitResult generation) {
    GenerationCommitResult exact = requireTopicCompacted(generation);
    return create(exact.streamId(), exact.coverage(), List.of(exact));
  }

  public KafkaCompactionGenerationSet extend(GenerationCommitResult generation) {
    GenerationCommitResult exact = requireTopicCompacted(generation);
    if (!exact.streamId().equals(streamId)
        || exact.coverage().startOffset() != coverage.endOffset()) {
      throw new IllegalArgumentException(
          "Kafka compaction extension does not continue the activated generation set");
    }
    ArrayList<GenerationCommitResult> extended = new ArrayList<>(generations);
    extended.add(exact);
    return create(
        streamId, new OffsetRange(coverage.startOffset(), exact.coverage().endOffset()), extended);
  }

  public static KafkaCompactionGenerationSet replacement(GenerationCommitResult generation) {
    return initial(generation);
  }

  public byte[] digestBytes() {
    return HexFormat.of().parseHex(digestSha256.value());
  }

  private static KafkaCompactionGenerationSet create(
      StreamId streamId, OffsetRange coverage, List<GenerationCommitResult> generations) {
    return new KafkaCompactionGenerationSet(
        streamId, coverage, generations, canonicalDigest(streamId, coverage, generations));
  }

  private static GenerationCommitResult requireTopicCompacted(GenerationCommitResult generation) {
    GenerationCommitResult exact = Objects.requireNonNull(generation, "generation");
    if (exact.view() != ReadView.TOPIC_COMPACTED) {
      throw new IllegalArgumentException("activated Kafka generation must use TOPIC_COMPACTED");
    }
    return exact;
  }

  private static Checksum canonicalDigest(
      StreamId streamId, OffsetRange coverage, List<GenerationCommitResult> generations) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream output = new DataOutputStream(bytes);
      writeText(output, "nereus-kafka-compaction-generation-set-v1");
      writeText(output, streamId.value());
      output.writeLong(coverage.startOffset());
      output.writeLong(coverage.endOffset());
      output.writeInt(generations.size());
      for (GenerationCommitResult generation : generations) {
        output.writeLong(generation.coverage().startOffset());
        output.writeLong(generation.coverage().endOffset());
        output.writeLong(generation.generation().value());
        writeText(output, generation.publicationId().value());
        writeText(output, generation.indexKey());
        output.writeLong(generation.indexMetadataVersion());
        writeText(output, generation.indexRecordSha256().value());
      }
      output.flush();
      return new Checksum(
          ChecksumType.SHA256,
          HexFormat.of()
              .formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray())));
    } catch (IOException failure) {
      throw new IllegalStateException("in-memory generation-set encoding failed", failure);
    } catch (NoSuchAlgorithmException failure) {
      throw new IllegalStateException("SHA-256 is unavailable", failure);
    }
  }

  private static void writeText(DataOutputStream output, String value) throws IOException {
    byte[] encoded = Objects.requireNonNull(value, "value").getBytes(StandardCharsets.UTF_8);
    output.writeInt(encoded.length);
    output.write(encoded);
  }
}
