/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.kafka.compaction;

import com.nereusstream.api.Checksum;
import com.nereusstream.api.ChecksumType;
import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.GenerationId;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.ObjectType;
import com.nereusstream.api.OffsetRange;
import com.nereusstream.api.PayloadFormat;
import com.nereusstream.api.PublicationId;
import com.nereusstream.api.ReadView;
import com.nereusstream.api.StreamId;
import com.nereusstream.api.target.ObjectSliceReadTarget;
import com.nereusstream.api.target.ReadTarget;
import com.nereusstream.core.read.GenerationReadConstraint;
import com.nereusstream.materialization.GenerationCommitResult;
import com.nereusstream.metadata.oxia.F4ScanToken;
import com.nereusstream.metadata.oxia.GenerationMetadataStore;
import com.nereusstream.metadata.oxia.GenerationScanPage;
import com.nereusstream.metadata.oxia.VersionedGenerationCandidate;
import com.nereusstream.metadata.oxia.VersionedGenerationIndex;
import com.nereusstream.metadata.oxia.codec.ReadTargetCodecRegistry;
import com.nereusstream.metadata.oxia.records.GenerationIndexRecord;
import com.nereusstream.metadata.oxia.records.GenerationLifecycle;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import com.nereusstream.objectstore.compacted.CompactedObjectFormatV2;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Reconstructs the unique gap-free NTC2 generation path named by a binding coverage digest. */
public final class KafkaActivatedGenerationSetResolver
    implements KafkaActivatedGenerationAuthority {
  static final int SCAN_PAGE_SIZE = 512;
  static final int MAX_CANDIDATES = 4_096;
  static final int MAX_SEARCH_NODES = 16_384;

  private static final Comparator<GenerationCommitResult> PATH_ORDER =
      Comparator.comparingLong((GenerationCommitResult value) -> value.coverage().endOffset())
          .thenComparingLong(value -> value.generation().value())
          .thenComparing(GenerationCommitResult::indexKey);

  private final String cluster;
  private final GenerationMetadataStore generations;
  private final ReadTargetCodecRegistry targetCodecs;

  public KafkaActivatedGenerationSetResolver(String cluster, GenerationMetadataStore generations) {
    this(cluster, generations, ReadTargetCodecRegistry.phase15());
  }

  KafkaActivatedGenerationSetResolver(
      String cluster, GenerationMetadataStore generations, ReadTargetCodecRegistry targetCodecs) {
    this.cluster = requireText(cluster, "cluster");
    this.generations = Objects.requireNonNull(generations, "generations");
    this.targetCodecs = Objects.requireNonNull(targetCodecs, "targetCodecs");
  }

  @Override
  public CompletableFuture<GenerationReadConstraint> resolve(
      StreamId streamId, KafkaCompactionCoverageRecord coverage) {
    StreamId exactStream = Objects.requireNonNull(streamId, "streamId");
    KafkaCompactionCoverageRecord exactCoverage = Objects.requireNonNull(coverage, "coverage");
    if (exactCoverage.coverageVersion() != 1) {
      return CompletableFuture.failedFuture(
          new NereusException(
              ErrorCode.INVALID_ARGUMENT,
              false,
              "activated generation discovery requires non-empty Kafka coverage"));
    }
    OffsetRange range = new OffsetRange(exactCoverage.startOffset(), exactCoverage.endOffset());
    return scan(
            exactStream,
            Math.addExact(range.startOffset(), 1),
            range.endOffset(),
            Optional.empty(),
            new ArrayList<>())
        .thenApply(
            wrappers ->
                select(
                    exactStream,
                    range,
                    sha256(exactCoverage.generationSetSha256()),
                    sha256(exactCoverage.policySha256()),
                    wrappers));
  }

  private CompletableFuture<List<VersionedGenerationCandidate>> scan(
      StreamId streamId,
      long minimumEnd,
      long maximumEnd,
      Optional<F4ScanToken> continuation,
      List<VersionedGenerationCandidate> accumulated) {
    int remaining = MAX_CANDIDATES - accumulated.size();
    if (remaining <= 0) {
      return limit("Kafka activated generation scan exceeded its candidate bound");
    }
    return generations
        .scanIndex(
            cluster,
            streamId,
            ReadView.TOPIC_COMPACTED,
            minimumEnd,
            maximumEnd,
            continuation,
            Math.min(SCAN_PAGE_SIZE, remaining))
        .thenCompose(page -> appendPage(streamId, minimumEnd, maximumEnd, accumulated, page));
  }

  private CompletableFuture<List<VersionedGenerationCandidate>> appendPage(
      StreamId streamId,
      long minimumEnd,
      long maximumEnd,
      List<VersionedGenerationCandidate> accumulated,
      GenerationScanPage page) {
    if (accumulated.size() + page.values().size() > MAX_CANDIDATES) {
      return limit("Kafka activated generation scan exceeded its candidate bound");
    }
    accumulated.addAll(page.values());
    if (page.continuation().isEmpty()) {
      return CompletableFuture.completedFuture(List.copyOf(accumulated));
    }
    if (accumulated.size() == MAX_CANDIDATES) {
      return limit("Kafka activated generation scan has additional candidates");
    }
    return scan(streamId, minimumEnd, maximumEnd, page.continuation(), accumulated);
  }

  private GenerationReadConstraint select(
      StreamId streamId,
      OffsetRange coverage,
      Checksum expectedSetSha256,
      Checksum expectedPolicySha256,
      List<VersionedGenerationCandidate> wrappers) {
    Map<Long, List<GenerationCommitResult>> byStart = new HashMap<>();
    for (VersionedGenerationCandidate candidate : wrappers) {
      if (!(candidate instanceof VersionedGenerationIndex index)) {
        continue;
      }
      Optional<GenerationCommitResult> admitted =
          admit(streamId, coverage, expectedPolicySha256, index);
      admitted.ifPresent(
          value ->
              byStart
                  .computeIfAbsent(value.coverage().startOffset(), ignored -> new ArrayList<>())
                  .add(value));
    }
    byStart.values().forEach(values -> values.sort(PATH_ORDER));
    Search search = new Search(expectedSetSha256);
    search(streamId, coverage, byStart, coverage.startOffset(), new ArrayList<>(), search);
    if (search.matches.isEmpty()) {
      throw invariant("Kafka binding generation-set digest has no exact committed NTC2 path");
    }
    if (search.matches.size() != 1) {
      throw invariant(
          "Kafka binding generation-set digest resolves to multiple committed NTC2 paths");
    }
    KafkaCompactionGenerationSet selected = search.matches.getFirst();
    List<GenerationReadConstraint.Identity> identities =
        selected.generations().stream()
            .map(
                value ->
                    new GenerationReadConstraint.Identity(
                        value.coverage(),
                        value.generation().value(),
                        value.publicationId(),
                        value.indexKey(),
                        value.indexMetadataVersion(),
                        value.indexRecordSha256()))
            .toList();
    return new GenerationReadConstraint(streamId, ReadView.TOPIC_COMPACTED, coverage, identities);
  }

  private Optional<GenerationCommitResult> admit(
      StreamId streamId,
      OffsetRange coverage,
      Checksum expectedPolicySha256,
      VersionedGenerationIndex index) {
    GenerationIndexRecord record = index.value();
    if (record.lifecycle() != GenerationLifecycle.COMMITTED
        || !record.streamId().equals(streamId.value())
        || record.readViewId() != ReadView.TOPIC_COMPACTED.wireId()
        || record.offsetStart() < coverage.startOffset()
        || record.offsetEnd() > coverage.endOffset()
        || !record.policySha256().equals(expectedPolicySha256.value())
        || !record.targetIdentitySha256().equals(record.readTarget().identityChecksumValue())
        || !record.payloadFormat().equals(PayloadFormat.KAFKA_RECORD_BATCH.name())) {
      return Optional.empty();
    }
    ReadTarget decoded;
    try {
      decoded = targetCodecs.decode(record.readTarget());
    } catch (RuntimeException failure) {
      return Optional.empty();
    }
    if (!(decoded instanceof ObjectSliceReadTarget target)
        || target.objectType() != ObjectType.STREAM_COMPACTED_OBJECT
        || !target.physicalFormat().equals(CompactedObjectFormatV2.TOPIC_COMPACTED_PHYSICAL_FORMAT)
        || !target.logicalFormat().equals(CompactedObjectFormatV2.KAFKA_LOGICAL_FORMAT)) {
      return Optional.empty();
    }
    return Optional.of(
        new GenerationCommitResult(
            streamId,
            ReadView.TOPIC_COMPACTED,
            new OffsetRange(record.offsetStart(), record.offsetEnd()),
            new GenerationId(record.generation()),
            new PublicationId(record.publicationId()),
            index.key(),
            index.metadataVersion(),
            index.durableValueSha256(),
            false));
  }

  private void search(
      StreamId streamId,
      OffsetRange coverage,
      Map<Long, List<GenerationCommitResult>> byStart,
      long cursor,
      ArrayList<GenerationCommitResult> path,
      Search search) {
    if (++search.nodes > MAX_SEARCH_NODES) {
      throw new NereusException(
          ErrorCode.METADATA_LIMIT_EXCEEDED,
          false,
          "Kafka activated generation path search exceeded its bound");
    }
    if (cursor == coverage.endOffset()) {
      KafkaCompactionGenerationSet generationSet = KafkaCompactionGenerationSet.of(path);
      if (generationSet.streamId().equals(streamId)
          && generationSet.coverage().equals(coverage)
          && generationSet.digestSha256().equals(search.expectedSha256)) {
        search.matches.add(generationSet);
      }
      return;
    }
    if (path.size() == KafkaCompactionGenerationSet.MAX_GENERATIONS) {
      return;
    }
    for (GenerationCommitResult candidate : byStart.getOrDefault(cursor, List.of())) {
      path.add(candidate);
      search(streamId, coverage, byStart, candidate.coverage().endOffset(), path, search);
      path.removeLast();
    }
  }

  private static Checksum sha256(byte[] value) {
    return new Checksum(ChecksumType.SHA256, HexFormat.of().formatHex(value));
  }

  private static <T> CompletableFuture<T> limit(String message) {
    return CompletableFuture.failedFuture(
        new NereusException(ErrorCode.METADATA_LIMIT_EXCEEDED, false, message));
  }

  private static NereusException invariant(String message) {
    return new NereusException(ErrorCode.METADATA_INVARIANT_VIOLATION, false, message);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.isBlank() || value.length() > 4_096) {
      throw new IllegalArgumentException(name + " must be non-blank and bounded");
    }
    return value;
  }

  private static final class Search {
    private final Checksum expectedSha256;
    private final ArrayList<KafkaCompactionGenerationSet> matches = new ArrayList<>();
    private int nodes;

    private Search(Checksum expectedSha256) {
      this.expectedSha256 = Objects.requireNonNull(expectedSha256, "expectedSha256");
    }
  }
}
