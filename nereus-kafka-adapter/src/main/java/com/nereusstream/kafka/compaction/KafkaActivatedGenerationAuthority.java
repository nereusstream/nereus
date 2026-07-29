/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 */
package com.nereusstream.kafka.compaction;

import com.nereusstream.api.ErrorCode;
import com.nereusstream.api.NereusException;
import com.nereusstream.api.StreamId;
import com.nereusstream.core.read.GenerationReadConstraint;
import com.nereusstream.metadata.oxia.KafkaPartitionId;
import com.nereusstream.metadata.oxia.VersionedKafkaPartitionBinding;
import com.nereusstream.metadata.oxia.records.KafkaCompactionCoverageRecord;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Resolves one binding-rooted Kafka compaction activation into exact durable generation identities.
 */
@FunctionalInterface
public interface KafkaActivatedGenerationAuthority {
  CompletableFuture<GenerationReadConstraint> resolve(
      StreamId streamId, KafkaCompactionCoverageRecord coverage);

  /**
   * Re-admits an exact physically repaired quarantined generation set and atomically replaces the
   * binding digest before a mandatory coordinator probe.
   */
  default CompletableFuture<VersionedKafkaPartitionBinding> repairIfQuarantined(
      KafkaPartitionId partition,
      StreamId streamId,
      VersionedKafkaPartitionBinding binding,
      Duration timeout) {
    Objects.requireNonNull(partition, "partition");
    Objects.requireNonNull(streamId, "streamId");
    Objects.requireNonNull(timeout, "timeout");
    return CompletableFuture.completedFuture(
        Objects.requireNonNull(binding, "binding"));
  }

  static KafkaActivatedGenerationAuthority unavailable() {
    return (streamId, coverage) ->
        CompletableFuture.failedFuture(
            new NereusException(
                ErrorCode.UNSUPPORTED_READ_SEMANTICS,
                false,
                "activated Kafka generation discovery is not configured"));
  }
}
