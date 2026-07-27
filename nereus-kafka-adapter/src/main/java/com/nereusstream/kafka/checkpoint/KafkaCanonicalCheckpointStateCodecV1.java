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

package com.nereusstream.kafka.checkpoint;

import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointFormatException;
import com.nereusstream.objectstore.kafka.checkpoint.KafkaCheckpointSection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Composition codec for the complete seven-section NKC1 V1 canonical image. */
public final class KafkaCanonicalCheckpointStateCodecV1 {
  private final KafkaProducerTransactionStateCodecV1 producerCodec;
  private final KafkaLeaderEpochStateCodecV1 leaderEpochCodec;
  private final KafkaVirtualSegmentStateCodecV1 virtualSegmentCodec;
  private final KafkaDerivedIndexStateCodecV1 derivedIndexCodec;

  public KafkaCanonicalCheckpointStateCodecV1() {
    this(
        new KafkaProducerTransactionStateCodecV1(),
        new KafkaLeaderEpochStateCodecV1(),
        new KafkaVirtualSegmentStateCodecV1(),
        new KafkaDerivedIndexStateCodecV1());
  }

  KafkaCanonicalCheckpointStateCodecV1(
      KafkaProducerTransactionStateCodecV1 producerCodec,
      KafkaLeaderEpochStateCodecV1 leaderEpochCodec,
      KafkaVirtualSegmentStateCodecV1 virtualSegmentCodec,
      KafkaDerivedIndexStateCodecV1 derivedIndexCodec) {
    this.producerCodec = Objects.requireNonNull(producerCodec, "producerCodec");
    this.leaderEpochCodec = Objects.requireNonNull(leaderEpochCodec, "leaderEpochCodec");
    this.virtualSegmentCodec = Objects.requireNonNull(virtualSegmentCodec, "virtualSegmentCodec");
    this.derivedIndexCodec = Objects.requireNonNull(derivedIndexCodec, "derivedIndexCodec");
  }

  public List<KafkaCheckpointSection> encodeSections(KafkaCanonicalCheckpointState state) {
    KafkaCanonicalCheckpointState exact = Objects.requireNonNull(state, "state");
    ArrayList<KafkaCheckpointSection> sections = new ArrayList<>(7);
    sections.addAll(
        producerCodec.encodeSections(exact.producerTransactionState(), exact.checkpointOffset()));
    sections.add(
        leaderEpochCodec.encodeSection(
            exact.leaderEpochState(), exact.logStartOffset(), exact.stableEndOffset()));
    sections.add(
        virtualSegmentCodec.encodeSection(
            exact.virtualSegmentState(), exact.logStartOffset(), exact.stableEndOffset()));
    sections.addAll(
        derivedIndexCodec.encodeSections(
            exact.derivedIndexState(), exact.logStartOffset(), exact.stableEndOffset()));
    sections.sort(Comparator.comparingInt(KafkaCheckpointSection::sectionType));
    return List.copyOf(sections);
  }

  public KafkaCanonicalCheckpointState decodeSections(
      List<KafkaCheckpointSection> sections,
      long checkpointOffset,
      long logStartOffset,
      long stableEndOffset) {
    if (checkpointOffset < 0
        || logStartOffset < 0
        || stableEndOffset < logStartOffset
        || checkpointOffset != stableEndOffset) {
      throw new IllegalArgumentException("invalid canonical Kafka checkpoint bounds");
    }
    try {
      List<KafkaCheckpointSection> exact =
          List.copyOf(Objects.requireNonNull(sections, "sections"));
      return new KafkaCanonicalCheckpointState(
          checkpointOffset,
          logStartOffset,
          stableEndOffset,
          producerCodec.decodeSections(exact, checkpointOffset),
          leaderEpochCodec.decodeSection(exact, logStartOffset, stableEndOffset),
          virtualSegmentCodec.decodeSection(exact, logStartOffset, stableEndOffset),
          derivedIndexCodec.decodeSections(exact, logStartOffset, stableEndOffset));
    } catch (KafkaCheckpointFormatException failure) {
      throw failure;
    } catch (IllegalArgumentException failure) {
      throw new KafkaCheckpointFormatException("malformed canonical NKC1 state", failure);
    }
  }
}
